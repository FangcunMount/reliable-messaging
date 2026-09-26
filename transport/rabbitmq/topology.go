package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
)

// ManagementVerifier uses only GET endpoints of the RabbitMQ management API.
// Its caller owns credentials, TLS settings, and the HTTP client lifecycle.
// Verification is a pre-publish observation, not an atomic routing guarantee.
type ManagementVerifier struct {
	baseURL  string
	vhost    string
	username string
	password string
	client   *http.Client
}

// NewManagementVerifier stores configuration without making a network call.
func NewManagementVerifier(baseURL, vhost, username, password string, client *http.Client) (*ManagementVerifier, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("absolute RabbitMQ management URL required")
	}
	if vhost == "" || username == "" || password == "" {
		return nil, errors.New("vhost and management credentials required")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &ManagementVerifier{
		baseURL: strings.TrimRight(baseURL, "/"), vhost: vhost,
		username: username, password: password, client: client,
	}, nil
}

func (v *ManagementVerifier) Verify(ctx context.Context, route Route) transport.Outcome {
	if v == nil || validateRoutes(map[string]Route{"route": route}) != nil {
		return transport.Rejected
	}
	vhost, exchange := url.PathEscape(v.vhost), url.PathEscape(route.Exchange)
	var exchangeInfo struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Durable bool   `json:"durable"`
	}
	outcome := v.get(ctx, "/api/exchanges/"+vhost+"/"+exchange, &exchangeInfo)
	if outcome != transport.Confirmed {
		return outcome
	}
	if exchangeInfo.Name != route.Exchange || exchangeInfo.Type != route.ExchangeKind || !exchangeInfo.Durable {
		return transport.Rejected
	}
	for _, required := range route.RequiredQueues {
		queue := url.PathEscape(required.Name)
		var queueInfo struct {
			Name    string `json:"name"`
			Type    string `json:"type"`
			Durable bool   `json:"durable"`
		}
		outcome = v.get(ctx, "/api/queues/"+vhost+"/"+queue, &queueInfo)
		if outcome != transport.Confirmed {
			return outcome
		}
		if queueInfo.Name != required.Name || queueInfo.Type != required.QueueType || !queueInfo.Durable {
			return transport.Rejected
		}
		var bindings []struct {
			Source      string `json:"source"`
			Destination string `json:"destination"`
			RoutingKey  string `json:"routing_key"`
		}
		outcome = v.get(ctx, "/api/bindings/"+vhost+"/e/"+exchange+"/q/"+queue, &bindings)
		if outcome != transport.Confirmed {
			return outcome
		}
		found := false
		for _, binding := range bindings {
			if binding.Source == route.Exchange && binding.Destination == required.Name && binding.RoutingKey == required.BindingKey {
				found = true
				break
			}
		}
		if !found {
			return transport.Rejected
		}
	}
	return transport.Confirmed
}

func (v *ManagementVerifier) get(ctx context.Context, path string, result any) transport.Outcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.baseURL+path, nil)
	if err != nil {
		return transport.Unknown
	}
	req.SetBasicAuth(v.username, v.password)
	req.Header.Set("Accept", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return transport.Unknown
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return transport.Rejected
	}
	if resp.StatusCode != http.StatusOK {
		return transport.Unknown
	}
	limited := io.LimitReader(resp.Body, 1<<20+1)
	if err := json.NewDecoder(limited).Decode(result); err != nil {
		return transport.Unknown
	}
	return transport.Confirmed
}

func bindingMatches(kind, bindingKey, routingKey string) bool {
	switch kind {
	case "fanout":
		return true
	case "direct":
		return bindingKey == routingKey
	case "topic":
		pattern, words := strings.Split(bindingKey, "."), strings.Split(routingKey, ".")
		memo := make(map[[2]int]bool)
		visited := make(map[[2]int]bool)
		var match func(int, int) bool
		match = func(i, j int) bool {
			state := [2]int{i, j}
			if visited[state] {
				return memo[state]
			}
			visited[state] = true
			if i == len(pattern) {
				memo[state] = j == len(words)
				return memo[state]
			}
			if pattern[i] == "#" {
				for n := j; n <= len(words); n++ {
					if match(i+1, n) {
						memo[state] = true
						return true
					}
				}
				return false
			}
			if j == len(words) {
				return false
			}
			memo[state] = (pattern[i] == "*" || pattern[i] == words[j]) && match(i+1, j+1)
			return memo[state]
		}
		return match(0, 0)
	default:
		return false
	}
}

var _ TopologyVerifier = (*ManagementVerifier)(nil)
