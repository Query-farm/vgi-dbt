// dbt Cloud HTTP client: the dbt_cloud secret, Admin API URLs, retries.

package dbt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// CloudSecretType is the DuckDB secret type holding dbt Cloud credentials.
	CloudSecretType = "dbt_cloud"
	// DefaultBaseURL is dbt Cloud's North America multi-tenant host.
	DefaultBaseURL = "https://cloud.getdbt.com"
	// MaxAttempts bounds retries of 429/5xx responses.
	MaxAttempts = 5
	// Timeout bounds every request.
	Timeout = 60 * time.Second
)

const userAgent = "vgi-dbt-worker/" + Version

func secretField(name, doc string, redact bool) arrow.Field {
	keys := []string{"vgi_doc"}
	vals := []string{doc}
	if redact {
		keys = append(keys, "redact")
		vals = append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata(keys, vals)}
}

// CloudSecretTypeSpec registers CREATE SECRET (TYPE dbt_cloud, ...).
var CloudSecretTypeSpec = vgi.SecretTypeSpec{
	Name:        CloudSecretType,
	Description: "dbt Cloud API credentials: a service or personal access token, the account id and the account's access URL.",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("token", "dbt Cloud service token or personal access token, sent as 'Authorization: Token <token>'.", true),
		secretField("account_id", "Numeric dbt Cloud account id (Account settings, or the number after /accounts/ in a dbt Cloud URL).", false),
		secretField("base_url", "Access URL of the account, e.g. https://cloud.getdbt.com (North America), https://emea.dbt.com, https://au.dbt.com, or a cell/single-tenant host such as https://ab123.us1.dbt.com. Default https://cloud.getdbt.com.", false),
		secretField("discovery_url", "Discovery (metadata) GraphQL endpoint. Derived from base_url when omitted, e.g. https://metadata.cloud.getdbt.com/graphql.", false),
		secretField("environment_id", "Default environment id for the Discovery API functions (cloud.models, cloud.tests, cloud.sources).", false),
	}, &cloudSecretMetadata),
}

var cloudSecretMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{"https://docs.getdbt.com/docs/dbt-cloud-apis/authentication"})

// cloudConfig is a resolved dbt_cloud secret. Never stored in scan state.
type cloudConfig struct {
	Token         string
	AccountID     string
	BaseURL       string
	DiscoveryURL  string
	EnvironmentID string
}

var errNoCloudSecret = errors.New("no dbt_cloud secret: run CREATE SECRET (TYPE dbt_cloud, TOKEN '...', ACCOUNT_ID '...') first")

// cloudConfigFrom reads the dbt_cloud secret.
func cloudConfigFrom(secrets vgi.Secrets) (*cloudConfig, error) {
	s, ok := secrets.Lookup(CloudSecretType)
	if !ok {
		return nil, errNoCloudSecret
	}
	c := &cloudConfig{
		Token:         strings.TrimSpace(vgi.RenderSecretValue(s["token"])),
		AccountID:     strings.TrimSpace(vgi.RenderSecretValue(s["account_id"])),
		BaseURL:       strings.TrimRight(strings.TrimSpace(vgi.RenderSecretValue(s["base_url"])), "/"),
		DiscoveryURL:  strings.TrimSpace(vgi.RenderSecretValue(s["discovery_url"])),
		EnvironmentID: strings.TrimSpace(vgi.RenderSecretValue(s["environment_id"])),
	}
	if c.BaseURL == "" {
		c.BaseURL = DefaultBaseURL
	}
	if c.Token == "" {
		return nil, errors.New("the dbt_cloud secret has no TOKEN")
	}
	if c.DiscoveryURL == "" {
		c.DiscoveryURL = deriveDiscoveryURL(c.BaseURL)
	}
	return c, nil
}

// requireAccount fails when the secret has no account id.
func (c *cloudConfig) requireAccount() error {
	if c.AccountID == "" {
		return errors.New("the dbt_cloud secret has no ACCOUNT_ID; list your accounts with dbt.cloud.accounts")
	}
	if _, err := strconv.ParseInt(c.AccountID, 10, 64); err != nil {
		return fmt.Errorf("dbt_cloud ACCOUNT_ID must be numeric, got %q", c.AccountID)
	}
	return nil
}

// deriveDiscoveryURL maps an access URL to its Discovery API endpoint:
// cloud.getdbt.com -> metadata.cloud.getdbt.com, emea.dbt.com ->
// metadata.emea.dbt.com, ab123.us1.dbt.com -> ab123.metadata.us1.dbt.com.
func deriveDiscoveryURL(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return strings.TrimRight(base, "/") + "/graphql"
	}
	host := u.Hostname()
	labels := strings.Split(host, ".")
	switch {
	case net.ParseIP(host) != nil || host == "localhost":
		// fixture / proxy servers: same host, /graphql path.
	case strings.HasPrefix(host, "metadata.") || strings.Contains(host, ".metadata."):
	case len(labels) >= 4 && strings.HasSuffix(host, ".dbt.com"):
		host = labels[0] + ".metadata." + strings.Join(labels[1:], ".")
	case strings.Contains(host, "."):
		host = "metadata." + host
	default:
		// localhost / fixture servers: same host, /graphql path.
	}
	if p := u.Port(); p != "" {
		host += ":" + p
	}
	return u.Scheme + "://" + host + "/graphql"
}

// headers are sent on every dbt Cloud request.
func (c *cloudConfig) headers() http.Header {
	h := http.Header{}
	h.Set("Authorization", "Token "+c.Token)
	return h
}

// adminURL builds <base>/api/<version>/accounts/<id>/<rest>?<q>; rest "" lists accounts.
func (c *cloudConfig) adminURL(version, rest string, q url.Values) string {
	u := c.BaseURL + "/api/" + version + "/accounts/"
	if c.AccountID != "" && rest != "" {
		u += c.AccountID + "/" + strings.TrimLeft(rest, "/")
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

var client = &http.Client{Timeout: Timeout}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// retryDelay honours an integer Retry-After header, else backs off exponentially.
func retryDelay(header string, attempt int) time.Duration {
	if s, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && s >= 0 {
		return min(time.Duration(s)*time.Second, 60*time.Second)
	}
	return time.Duration(500<<attempt) * time.Millisecond
}

// doRequest sends a request, retrying transport errors, 429 and 5xx. The
// caller closes the returned body.
func doRequest(ctx context.Context, method, rawURL string, header http.Header, body []byte) (io.ReadCloser, int, error) {
	for attempt := 1; ; attempt++ {
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, rd)
		if err != nil {
			return nil, 0, err
		}
		for k, vs := range header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", userAgent)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			if attempt >= MaxAttempts || errors.Is(err, context.Canceled) {
				return nil, 0, fmt.Errorf("%s %s failed: %w", method, redactURL(rawURL), err)
			}
			sleep(ctx, retryDelay("", attempt))
			continue
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		if retryable && attempt < MaxAttempts {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			sleep(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		return resp.Body, resp.StatusCode, nil
	}
}

// httpGet is a GET with retries.
func httpGet(ctx context.Context, rawURL string, header http.Header) (io.ReadCloser, int, error) {
	return doRequest(ctx, http.MethodGet, rawURL, header, nil)
}

// fetchJSON performs a request and returns the body of a 200 response.
func fetchJSON(ctx context.Context, method, rawURL string, header http.Header, body []byte) ([]byte, error) {
	rc, status, err := doRequest(ctx, method, rawURL, header, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, 256<<20))
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		snippet := strings.TrimSpace(string(data))
		if len(snippet) > 500 {
			snippet = snippet[:500]
		}
		return nil, fmt.Errorf("%s %s failed with HTTP %d %s: %s", method, redactURL(rawURL), status, http.StatusText(status), snippet)
	}
	return data, nil
}

// adminPage is the dbt Cloud Admin API envelope.
type adminPage struct {
	Data  json.RawMessage `json:"data"`
	Extra struct {
		Pagination struct {
			Count      int64 `json:"count"`
			TotalCount int64 `json:"total_count"`
		} `json:"pagination"`
	} `json:"extra"`
}

// adminGet fetches one Admin API page and returns its data items (a single
// object is returned as a one-item list).
func (c *cloudConfig) adminGet(ctx context.Context, rawURL string) ([]json.RawMessage, int64, error) {
	body, err := fetchJSON(ctx, http.MethodGet, rawURL, c.headers(), nil)
	if err != nil {
		return nil, 0, err
	}
	var p adminPage
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, 0, fmt.Errorf("decoding dbt Cloud response: %w", err)
	}
	if isNullRaw(p.Data) {
		return nil, p.Extra.Pagination.TotalCount, nil
	}
	if p.Data[0] != '[' {
		return []json.RawMessage{p.Data}, 1, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(p.Data, &items); err != nil {
		return nil, 0, fmt.Errorf("decoding dbt Cloud data: %w", err)
	}
	return items, p.Extra.Pagination.TotalCount, nil
}
