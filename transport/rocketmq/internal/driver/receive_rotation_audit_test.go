package golang

import (
	"context"
	"io"
	"testing"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

type rotationAuditManager struct {
	ClientManager
	topics []string
}

func (m *rotationAuditManager) ReceiveMessage(_ context.Context, _ *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	m.topics = append(m.topics, request.GetMessageQueue().GetTopic().GetName())
	return &rotationAuditStream{}, nil
}

type rotationAuditStream struct {
	v2.MessagingService_ReceiveMessageClient
	emitted bool
}

func (s *rotationAuditStream) Recv() (*v2.ReceiveMessageResponse, error) {
	if s.emitted {
		return nil, io.EOF
	}
	s.emitted = true
	return &v2.ReceiveMessageResponse{Content: &v2.ReceiveMessageResponse_Status{Status: &v2.Status{Code: v2.Code_OK}}}, nil
}

// Calls the actual consumer and RPC request selection. The RPC stream is empty
// to isolate input selection from Broker/network behavior. Native Broker and
// host acceptance are recorded separately.
func TestReceiveCoversEachPhysicalInputPerRotation(t *testing.T) {
	manager := &rotationAuditManager{}
	client := &defaultClient{config: &Config{}, clientManager: manager, opts: clientOptions{timeout: time.Second}}
	client.on.Store(true)
	expressions := map[string]*FilterExpression{"input-mysql": SUB_ALL, "input-mongo": SUB_ALL}
	consumer := &defaultSimpleConsumer{cli: client, groupName: "rotation-audit", subscriptionExpressions: &expressions, receiveRateLimiter: newReceiveRateLimiter(1)}
	for name := range expressions {
		consumer.subTopicRouteDataResultCache.Store(name, &subscriptionLoadBalancer{messageQueues: []*v2.MessageQueue{{Topic: &v2.Resource{Name: name}, Broker: &v2.Broker{Endpoints: &v2.Endpoints{}}}}})
	}
	for cycle := 0; cycle < 128; cycle++ {
		for range 2 {
			if _, err := consumer.Receive(context.Background(), 1, time.Minute); err != nil {
				t.Fatal(err)
			}
		}
		last := manager.topics[len(manager.topics)-2:]
		if last[0] == last[1] {
			t.Fatalf("fixed physical inputs not covered in rotation %d: requests=%v", cycle, last)
		}
	}
}
