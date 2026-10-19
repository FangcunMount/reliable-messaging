package rocketmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/FangcunMount/reliable-messaging/transaction"
	"github.com/FangcunMount/reliable-messaging/transport"
	driver "github.com/FangcunMount/reliable-messaging/transport/rocketmq/internal/driver"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const TransactionReferenceProperty = "rm_tx_ref"

// TransactionRoute comes only from the reviewed manifest, never a request body.
type TransactionRoute struct {
	LogicalTopic  string `json:"topic"`
	PhysicalTopic string `json:"physical_topic"`
	Revision      string `json:"route_revision"`
}
type TransactionConfig struct {
	TLS                                   *tls.Config
	Endpoint, AccessKey, SecretKey, Owner string
	Routes                                []TransactionRoute
	Limits                                transaction.Limits
}
type transactionHandle interface {
	Finalize(context.Context, transaction.Decision) error
}
type transactionBackend interface {
	Start(context.Context) error
	Prepare(context.Context, transaction.Reference, []byte) (transaction.HalfReceipt, transactionHandle, error)
	Close() error
}
type backendFactory func(func(func()) bool, func(context.Context, *driver.MessageView) driver.TransactionResolution) (transactionBackend, error)

func validateTransactionConfig(c TransactionConfig, store transaction.PreparedStore) (TransactionConfig, error) {
	host, port, err := net.SplitHostPort(c.Endpoint)
	if err != nil || host == "" {
		return c, transaction.ErrBinding
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return c, transaction.ErrBinding
	}
	if c.Limits == (transaction.Limits{}) {
		c.Limits = transaction.DefaultLimits()
	}
	if store == nil || !c.Limits.Valid() || c.Owner == "" || store.Binding().Owner != c.Owner || c.AccessKey == "" || c.SecretKey == "" || len(c.Routes) == 0 || len(c.Routes) > 256 {
		return c, transaction.ErrBinding
	}
	if c.TLS == nil {
		c.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		c.TLS = c.TLS.Clone()
	}
	if c.TLS.InsecureSkipVerify {
		return c, errors.New("transaction client requires verified TLS")
	}
	physical := map[string]bool{}
	logical := map[[2]string]bool{}
	for _, r := range c.Routes {
		expected, err := transport.PhysicalTopicV2(r.LogicalTopic, transport.TransactionMessageType, c.Owner, r.Revision)
		if err != nil || r.PhysicalTopic != expected {
			return c, transaction.ErrBinding
		}
		if r.LogicalTopic == "" || r.PhysicalTopic == "" || r.Revision == "" || physical[r.PhysicalTopic] || logical[[2]string{r.LogicalTopic, r.Revision}] {
			return c, transaction.ErrBinding
		}
		physical[r.PhysicalTopic] = true
		logical[[2]string{r.LogicalTopic, r.Revision}] = true
	}
	c.Routes = append([]TransactionRoute(nil), c.Routes...)
	return c, nil
}

type driverTransactionBackend struct {
	inner   driver.ContextProducer
	started bool
}
type driverTransactionHandle struct{ inner driver.ContextTransaction }

func (h driverTransactionHandle) Finalize(ctx context.Context, d transaction.Decision) error {
	switch d {
	case transaction.Commit:
		return h.inner.CommitContext(ctx)
	case transaction.Rollback:
		return h.inner.RollBackContext(ctx)
	}
	return transaction.ErrInvalidPlan
}
func (s *TransactionalSender) driverFactory(dispatch func(func()) bool, check func(context.Context, *driver.MessageView) driver.TransactionResolution) (transactionBackend, error) {
	topics := make([]string, 0, len(s.config.Routes))
	for _, r := range s.config.Routes {
		topics = append(topics, r.PhysicalTopic)
	}
	inner, err := driver.NewProducer(&driver.Config{Endpoint: s.config.Endpoint, Credentials: &credentials.SessionCredentials{AccessKey: s.config.AccessKey, AccessSecret: s.config.SecretKey}}, driver.WithTopics(topics...), driver.WithMaxAttempts(1), driver.WithTransactionChecker(&driver.TransactionChecker{CheckContext: check, Dispatch: dispatch}), driver.WithClientFunc(func(c *driver.Config, opts ...driver.ClientOption) (driver.Client, error) {
		return driver.NewClient(c, append(opts, driver.WithQueryRouteTimeout(s.config.Limits.RPCTimeout), driver.WithConnOptions(driver.WithTLSConfig(s.config.TLS), driver.WithDialTimeout(s.config.Limits.RPCTimeout)))...)
	}))
	if err != nil {
		return nil, err
	}
	inner.(interface{ SetRequestTimeout(time.Duration) }).SetRequestTimeout(s.config.Limits.RPCTimeout)
	return &driverTransactionBackend{inner: inner.(driver.ContextProducer)}, nil
}
func (b *driverTransactionBackend) Start(ctx context.Context) error {
	err := b.inner.StartContext(ctx)
	b.started = err == nil
	return err
}
func (b *driverTransactionBackend) Close() error {
	if !b.started {
		return nil
	}
	b.started = false
	return b.inner.GracefulStop()
}
func (b *driverTransactionBackend) Prepare(ctx context.Context, ref transaction.Reference, wire []byte) (transaction.HalfReceipt, transactionHandle, error) {
	raw, err := transaction.EncodeReference(ref)
	if err != nil {
		return transaction.HalfReceipt{}, nil, err
	}
	msg := &driver.Message{Topic: ref.PhysicalTopic, Body: append([]byte(nil), wire...)}
	msg.SetKeys(ref.MessageID)
	msg.AddProperty(TransactionReferenceProperty, string(raw))
	tx := b.inner.BeginTransaction().(driver.ContextTransaction)
	receipts, err := b.inner.SendWithTransaction(ctx, msg, tx)
	h := transaction.HalfReceipt{Outcome: transaction.HalfUnknown, ErrorCode: "half_unknown"}
	if code := rejectedHalfCode(err); code != "" {
		h.Outcome, h.ErrorCode = transaction.HalfRejected, code
	}
	if err == nil && len(receipts) == 1 && receipts[0] != nil && receipts[0].MessageID != "" && receipts[0].TransactionId != "" {
		h = transaction.HalfReceipt{Outcome: transaction.HalfPrepared, BrokerMessageID: receipts[0].MessageID, BrokerTransactionID: receipts[0].TransactionId}
		return h, driverTransactionHandle{tx}, nil
	}
	if err == nil {
		err = errors.New("transaction half receipt missing identity")
	}
	return h, nil, err
}

func rejectedHalfCode(err error) string {
	var status *driver.ErrRpcStatus
	if errors.As(err, &status) && (status.Code >= 40000 && status.Code < 40600 || status.Code >= 41300 && status.Code < 41400) {
		return fmt.Sprintf("half_rejected_%d", status.Code)
	}
	switch grpcstatus.Code(err) {
	case codes.PermissionDenied:
		return "half_rejected_grpc_permission_denied"
	case codes.Unauthenticated:
		return "half_rejected_grpc_unauthenticated"
	default:
		return ""
	}
}
