package transport

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRoutingV2SharedCanonicalVectors(t *testing.T) {
	raw, err := os.ReadFile("../contracts/rocketmq-routing-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Version int            `json:"version"`
		Vectors []RouteBinding `json:"vectors"`
	}
	if err = json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if vectors.Version != 2 || len(vectors.Vectors) != 6 {
		t.Fatal("required nonempty v2 vectors")
	}
	for _, r := range vectors.Vectors {
		if err := ValidateRoutes([]RouteBinding{r}); err != nil {
			t.Fatal(r, err)
		}
	}
	if err := ValidateRoutes(vectors.Vectors); err != nil {
		t.Fatal(err)
	}
	first, tx, otherOwner := vectors.Vectors[0], vectors.Vectors[1], vectors.Vectors[2]
	if first.PhysicalTopic == tx.PhysicalTopic || tx.PhysicalTopic == otherOwner.PhysicalTopic || first.ConsumerGroup != tx.ConsumerGroup || tx.ConsumerGroup != otherOwner.ConsumerGroup {
		t.Fatal("type/owner isolation or logical subscription identity lost")
	}
	tx.Owner = "iam.mysql"
	if err := ValidateRoutes([]RouteBinding{tx}); err == nil {
		t.Fatal("wrong owner kept old physical route")
	}
	if _, err := PhysicalTopicV2("topic", "FIFO", "qs.mysql", "r1"); err == nil {
		t.Fatal("unsupported type admitted")
	}
	if _, err := PhysicalTopicV2("topic\x00suffix", TransactionMessageType, "qs.mysql", "r1"); err == nil {
		t.Fatal("NUL field admitted")
	}
	if _, err := ConsumerGroupV2("topic", ""); err == nil {
		t.Fatal("empty subscription admitted")
	}
}
