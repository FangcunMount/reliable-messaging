package catalog

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config 是从 YAML 加载的事件契约根配置。
type Config struct {
	Version string                 `yaml:"version"`
	Topics  map[string]TopicConfig `yaml:"topics"`
	Events  map[string]EventConfig `yaml:"events"`
}

// TopicConfig 描述一个逻辑事件 topic。
type TopicConfig struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// DeliveryClass 描述一个事件类型的投递可靠性契约。
type DeliveryClass string

const (
	DeliveryClassBestEffort    DeliveryClass = "best_effort"
	DeliveryClassDurableOutbox DeliveryClass = "durable_outbox"
)

// Valid 判断投递等级是否属于当前支持的契约。
func (c DeliveryClass) Valid() bool {
	switch c {
	case DeliveryClassBestEffort, DeliveryClassDurableOutbox:
		return true
	default:
		return false
	}
}

func (c DeliveryClass) String() string {
	return string(c)
}

// EventConfig 描述一个事件类型及其运行时路由契约。
type EventConfig struct {
	Topic       string        `yaml:"topic"`
	Delivery    DeliveryClass `yaml:"delivery"`
	Aggregate   string        `yaml:"aggregate"`
	Domain      string        `yaml:"domain"`
	Description string        `yaml:"description"`
	Handler     string        `yaml:"handler"`
}

// ValidateOptions controls optional catalog policies. The zero value keeps only
// structural validation, while Validate uses StrictValidateOptions for backward
// compatible service startup checks.
type ValidateOptions struct {
	RequireHandler         bool
	RequireTopicReferenced bool
}

var StrictValidateOptions = ValidateOptions{
	RequireHandler:         true,
	RequireTopicReferenced: true,
}

// Load 从磁盘读取并校验事件目录。
func Load(path string) (*Config, error) {
	return LoadWithOptions(path, StrictValidateOptions)
}

// LoadWithOptions 从磁盘读取并使用指定策略校验事件目录。
func LoadWithOptions(path string, opts ValidateOptions) (*Config, error) {
	// #nosec G304 -- 配置路径由可信的服务启动参数提供。
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return ParseWithOptions(data, opts)
}

// Parse 解码并校验事件目录。
func Parse(data []byte) (*Config, error) {
	return ParseWithOptions(data, StrictValidateOptions)
}

// ParseWithOptions 解码并使用指定策略校验事件目录。
func ParseWithOptions(data []byte, opts ValidateOptions) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if err := cfg.ValidateWithOptions(opts); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	return &cfg, nil
}

// Validate 校验 topic、handler 和 delivery 引用。
func (c *Config) Validate() error {
	return c.ValidateWithOptions(StrictValidateOptions)
}

// ValidateWithOptions 校验 topic、delivery 引用，并按策略校验 handler 和 topic 使用情况。
func (c *Config) ValidateWithOptions(opts ValidateOptions) error {
	referencedTopics := make(map[string]struct{}, len(c.Topics))

	for eventType, eventCfg := range c.Events {
		if _, ok := c.Topics[eventCfg.Topic]; !ok {
			return fmt.Errorf("event %q references unknown topic %q", eventType, eventCfg.Topic)
		}
		if opts.RequireHandler && eventCfg.Handler == "" {
			return fmt.Errorf("event %q has empty handler", eventType)
		}
		if eventCfg.Delivery == "" {
			return fmt.Errorf("event %q has empty delivery", eventType)
		}
		if !eventCfg.Delivery.Valid() {
			return fmt.Errorf("event %q has invalid delivery %q", eventType, eventCfg.Delivery)
		}
		referencedTopics[eventCfg.Topic] = struct{}{}
	}

	if opts.RequireTopicReferenced {
		for topicKey := range c.Topics {
			if _, ok := referencedTopics[topicKey]; !ok {
				return fmt.Errorf("topic %q has no events", topicKey)
			}
		}
	}
	return nil
}

// GetTopicName 返回事件类型对应的物理 topic 名称。
func (c *Config) GetTopicName(eventType string) (string, bool) {
	eventCfg, ok := c.Events[eventType]
	if !ok {
		return "", false
	}
	topicCfg, ok := c.Topics[eventCfg.Topic]
	if !ok {
		return "", false
	}
	return topicCfg.Name, true
}

// GetEventsByTopic 返回绑定到指定逻辑 topic key 的事件类型。
func (c *Config) GetEventsByTopic(topicKey string) []string {
	var events []string
	for eventType, eventCfg := range c.Events {
		if eventCfg.Topic == topicKey {
			events = append(events, eventType)
		}
	}
	return events
}

// GetTopicKeys 返回所有逻辑 topic key。
func (c *Config) GetTopicKeys() []string {
	keys := make([]string, 0, len(c.Topics))
	for k := range c.Topics {
		keys = append(keys, k)
	}
	return keys
}

// GetHandlerName 返回事件类型配置的 handler 名称。
func (c *Config) GetHandlerName(eventType string) (string, bool) {
	eventCfg, ok := c.Events[eventType]
	if !ok {
		return "", false
	}
	return eventCfg.Handler, eventCfg.Handler != ""
}

// GetDeliveryClass 返回事件类型配置的投递等级。
func (c *Config) GetDeliveryClass(eventType string) (DeliveryClass, bool) {
	eventCfg, ok := c.Events[eventType]
	if !ok || eventCfg.Delivery == "" {
		return "", false
	}
	return eventCfg.Delivery, true
}

// ListEventTypes 返回目录中声明的所有事件类型。
func (c *Config) ListEventTypes() []string {
	types := make([]string, 0, len(c.Events))
	for t := range c.Events {
		types = append(types, t)
	}
	return types
}
