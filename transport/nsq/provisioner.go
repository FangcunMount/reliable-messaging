package nsq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	driver "github.com/nsqio/go-nsq"
)

// Provisioner prepares topics and durable channels on every declared nsqd.
// Endpoints are explicit HTTP URLs; no TCP-to-HTTP port inference is made.
// The host owns the client, the endpoint list, and the business topic catalog.
// Construction performs no network work. A partial failure is returned so the
// host can stop startup instead of publishing to an unprepared node.
type Provisioner struct {
	client    *http.Client
	endpoints []url.URL
}

func NewProvisioner(client *http.Client, nsqdHTTP []string) (*Provisioner, error) {
	if client == nil || len(nsqdHTTP) == 0 {
		return nil, errors.New("host HTTP client and nsqd HTTP endpoints required")
	}
	endpoints := make([]url.URL, 0, len(nsqdHTTP))
	seen := make(map[string]struct{}, len(nsqdHTTP))
	for _, raw := range nsqdHTTP {
		endpoint, err := url.Parse(raw)
		if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.Hostname() == "" || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return nil, fmt.Errorf("explicit nsqd HTTP URL required: %q", raw)
		}
		_, port, err := net.SplitHostPort(endpoint.Host)
		if err != nil {
			return nil, fmt.Errorf("nsqd HTTP port required: %q: %w", raw, err)
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, fmt.Errorf("invalid nsqd HTTP port: %q", raw)
		}
		key := endpoint.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		endpoints = append(endpoints, *endpoint)
	}
	return &Provisioner{client: client, endpoints: endpoints}, nil
}

func (p *Provisioner) EnsureTopic(ctx context.Context, topic string) error {
	if !driver.IsValidTopicName(topic) {
		return errors.New("valid NSQ topic required")
	}
	return p.ensure(ctx, "/topic/create", url.Values{"topic": {topic}})
}

// EnsureChannel creates the topic and then a durable channel before the first
// publish. The caller must list every nsqd on which its publisher can send and
// rerun preparation when that set changes.
func (p *Provisioner) EnsureChannel(ctx context.Context, topic, channel string) error {
	if !driver.IsValidTopicName(topic) || !driver.IsValidChannelName(channel) || strings.HasSuffix(channel, "#ephemeral") {
		return errors.New("valid NSQ topic and durable channel required")
	}
	if err := p.EnsureTopic(ctx, topic); err != nil {
		return err
	}
	return p.ensure(ctx, "/channel/create", url.Values{"topic": {topic}, "channel": {channel}})
}

func (p *Provisioner) ensure(ctx context.Context, path string, query url.Values) error {
	for _, base := range p.endpoints {
		if err := ctx.Err(); err != nil {
			return err
		}
		endpoint := base
		endpoint.Path = path
		endpoint.RawQuery = query.Encode()
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.String(), nil)
		if err != nil {
			cancel()
			return err
		}
		response, err := p.client.Do(request)
		if err != nil {
			cancel()
			return fmt.Errorf("prepare NSQ resource on %s: %w", base.Host, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		closeErr := response.Body.Close()
		cancel()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("prepare NSQ resource on %s: status %s: %s", base.Host, response.Status, strings.TrimSpace(string(body)))
		}
		if err := errors.Join(readErr, closeErr); err != nil {
			return fmt.Errorf("read NSQ preparation response on %s: %w", base.Host, err)
		}
	}
	return nil
}
