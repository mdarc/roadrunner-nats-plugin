package natsjobs

import (
	"github.com/nats-io/nats.go"
)

const (
	pipeSubject            string = "subject"
	pipeStream             string = "stream"
	pipePrefetch           string = "prefetch"
	pipeDeleteAfterAck     string = "delete_after_ack"
	pipeDeliverNew         string = "deliver_new"
	pipeDeliverLast        string = "deliver_last"
	pipeRateLimit          string = "rate_limit"
	pipeDeleteStreamOnStop string = "delete_stream_on_stop"
	pipeConsumeAll         string = "consume_all"
	pipeDurable            string = "durable"
)

type config struct {
	// global
	// NATS URL
	Addr     string `mapstructure:"addr"`
	Token    string `mapstructure:"token"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"` // Client name

	ConsumeAll         bool   `mapstructure:"consume_all"`
	Priority           int64  `mapstructure:"priority"`
	Subject            string `mapstructure:"subject"`
	Stream             string `mapstructure:"stream"`
	Prefetch           int    `mapstructure:"prefetch"`
	RateLimit          uint64 `mapstructure:"rate_limit"`
	DeleteAfterAck     bool   `mapstructure:"delete_after_ack"`
	DeliverNew         bool   `mapstructure:"deliver_new"`
	DeliverLast        bool   `mapstructure:"deliver_last"`
	DeleteStreamOnStop bool   `mapstructure:"delete_stream_on_stop"`
	Durable            string `mapstructure:"durable"` // The name of a durable consumer name
}

func (c *config) InitDefaults() {
	if c.Addr == "" {
		c.Addr = nats.DefaultURL
	}

	if c.RateLimit == 0 {
		c.RateLimit = 1000
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.Stream == "" {
		c.Stream = "default-stream"
	}

	if c.Subject == "" {
		c.Subject = "default"
	}

	if c.Prefetch == 0 {
		c.Prefetch = 10
	}
}
