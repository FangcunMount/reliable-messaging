package nsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	driver "github.com/nsqio/go-nsq"
)

type lookupdProducer struct {
	BroadcastAddress string `json:"broadcast_address"`
	Hostname         string `json:"hostname"`
	TCPPort          int    `json:"tcp_port"`
}

type lookupdResponse struct {
	Producers []lookupdProducer `json:"producers"`
}

// resolveTopicProducers finds the source nsqd nodes so each failure consumer
// has a channel on them before business consumption starts. Later nodes are
// connected by DirectHandoff.Ready before their first terminal publish.
func resolveTopicProducers(ctx context.Context, lookupdAddresses []string, topic string) ([]string, error) {
	if !driver.IsValidTopicName(topic) {
		return nil, errors.New("valid topic and lookupd addresses required")
	}
	return resolveLookupdProducers(ctx, lookupdAddresses, "/lookup", topic)
}

// resolveBootstrapSources allows a subscription to start before the business
// topic has appeared in lookupd. One active nsqd establishes the failure
// channel; lookupd polling and DirectHandoff.Ready cover later source nodes.
func resolveBootstrapSources(ctx context.Context, lookupdAddresses []string, topic string) ([]string, error) {
	addresses, topicErr := resolveTopicProducers(ctx, lookupdAddresses, topic)
	if topicErr == nil {
		return addresses, nil
	}
	nodes, nodesErr := resolveLookupdProducers(ctx, lookupdAddresses, "/nodes", "")
	if nodesErr != nil {
		return nil, errors.Join(topicErr, fmt.Errorf("resolve active NSQD nodes: %w", nodesErr))
	}
	return nodes[:1], nil
}

func resolveLookupdProducers(ctx context.Context, lookupdAddresses []string, path, topic string) ([]string, error) {
	if len(lookupdAddresses) == 0 {
		return nil, errors.New("lookupd addresses required")
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	found := make(map[string]struct{})
	var failures []error
	type result struct {
		address   string
		producers []lookupdProducer
		err       error
	}
	results := make(chan result, len(lookupdAddresses))
	for _, address := range lookupdAddresses {
		go func(address string) {
			producers, err := queryLookupd(queryCtx, client, address, path, topic)
			results <- result{address: address, producers: producers, err: err}
		}(address)
	}
	for range lookupdAddresses {
		response := <-results
		if response.err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", response.address, response.err))
			continue
		}
		for _, producer := range response.producers {
			host := strings.TrimSpace(producer.BroadcastAddress)
			if host == "" {
				host = strings.TrimSpace(producer.Hostname)
			}
			if host == "" || producer.TCPPort < 1 || producer.TCPPort > 65535 {
				continue
			}
			found[net.JoinHostPort(host, strconv.Itoa(producer.TCPPort))] = struct{}{}
		}
	}
	if len(found) == 0 {
		if len(failures) == 0 {
			return nil, fmt.Errorf("no lookupd returned an NSQD for %s", path)
		}
		return nil, fmt.Errorf("no lookupd returned an NSQD for %s: %w", path, errors.Join(failures...))
	}
	addresses := make([]string, 0, len(found))
	for address := range found {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses, nil
}

func queryLookupd(ctx context.Context, client *http.Client, address, path, topic string) ([]lookupdProducer, error) {
	base := strings.TrimSpace(address)
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	endpoint, err := url.Parse(base)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, errors.New("invalid lookupd address")
	}
	endpoint.Path = path
	query := endpoint.Query()
	if topic != "" {
		query.Set("topic", topic)
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("lookupd status %s", response.Status)
	}
	var payload lookupdResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode lookupd producers: %w", err)
	}
	return payload.Producers, nil
}
