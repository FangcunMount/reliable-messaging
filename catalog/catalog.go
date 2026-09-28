package catalog

// Catalog 是事件契约的不可变查询视图。
type Catalog struct {
	config        *Config
	eventToTopic  map[string]string
	topicToEvents map[string][]string
}

// TopicResolver 将事件类型解析为物理 topic 名称。
type TopicResolver interface {
	GetTopicForEvent(eventType string) (string, bool)
}

// DeliveryClassResolver 将事件类型解析为投递可靠性契约。
type DeliveryClassResolver interface {
	GetDeliveryClass(eventType string) (DeliveryClass, bool)
}

// NewCatalog 根据已校验配置构建查询目录。
func NewCatalog(cfg *Config) *Catalog {
	c := &Catalog{
		config:        cfg,
		eventToTopic:  make(map[string]string),
		topicToEvents: make(map[string][]string),
	}
	if cfg == nil {
		return c
	}
	for eventType, eventCfg := range cfg.Events {
		if topicCfg, ok := cfg.Topics[eventCfg.Topic]; ok {
			topicName := topicCfg.Name
			c.eventToTopic[eventType] = topicName
			c.topicToEvents[topicName] = append(c.topicToEvents[topicName], eventType)
		}
	}
	return c
}

// TopicSubscription 描述一个由目录推导出的 topic 订阅。
type TopicSubscription struct {
	TopicName  string
	TopicKey   string
	EventTypes []string
}

// Config 返回目录配置。
func (c *Catalog) Config() *Config {
	if c == nil {
		return nil
	}
	return c.config
}

// GetTopicForEvent 返回事件类型对应的物理 topic 名称。
func (c *Catalog) GetTopicForEvent(eventType string) (string, bool) {
	if c == nil {
		return "", false
	}
	topic, ok := c.eventToTopic[eventType]
	return topic, ok
}

// GetEventsForTopic 返回绑定到指定物理 topic 名称的事件类型。
func (c *Catalog) GetEventsForTopic(topicName string) []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.topicToEvents[topicName]...)
}

// GetTopicConfig 返回逻辑 topic 配置。
func (c *Catalog) GetTopicConfig(topicKey string) (TopicConfig, bool) {
	if c == nil || c.config == nil {
		return TopicConfig{}, false
	}
	cfg, ok := c.config.Topics[topicKey]
	return cfg, ok
}

// GetEventConfig 返回事件配置。
func (c *Catalog) GetEventConfig(eventType string) (EventConfig, bool) {
	if c == nil || c.config == nil {
		return EventConfig{}, false
	}
	cfg, ok := c.config.Events[eventType]
	return cfg, ok
}

// GetDeliveryClass 返回事件类型配置的投递等级。
func (c *Catalog) GetDeliveryClass(eventType string) (DeliveryClass, bool) {
	if c == nil || c.config == nil {
		return "", false
	}
	return c.config.GetDeliveryClass(eventType)
}

// IsDurableOutbox 判断事件是否必须经过 outbox 暂存。
func (c *Catalog) IsDurableOutbox(eventType string) bool {
	delivery, ok := c.GetDeliveryClass(eventType)
	return ok && delivery == DeliveryClassDurableOutbox
}

// AllTopicNames 返回所有包含事件的物理 topic 名称。
func (c *Catalog) AllTopicNames() []string {
	if c == nil {
		return nil
	}
	names := make([]string, 0, len(c.topicToEvents))
	for name := range c.topicToEvents {
		names = append(names, name)
	}
	return names
}

// IsEventRegistered 判断事件类型是否已在目录中注册。
func (c *Catalog) IsEventRegistered(eventType string) bool {
	if c == nil {
		return false
	}
	_, ok := c.eventToTopic[eventType]
	return ok
}

// TopicSubscriptions 返回所有至少包含一个事件的 topic 订阅。
func (c *Catalog) TopicSubscriptions() []TopicSubscription {
	if c == nil || c.config == nil {
		return nil
	}
	subs := make([]TopicSubscription, 0, len(c.config.Topics))
	for topicKey, topicCfg := range c.config.Topics {
		events := c.config.GetEventsByTopic(topicKey)
		if len(events) == 0 {
			continue
		}
		subs = append(subs, TopicSubscription{
			TopicName:  topicCfg.Name,
			TopicKey:   topicKey,
			EventTypes: events,
		})
	}
	return subs
}
