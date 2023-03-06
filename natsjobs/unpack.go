package natsjobs

import (
	"fmt"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

const (
	// consume all
	auto string = "deduced_by_rr"
)

func (c *Driver) unpack(m *nats.Msg, meta *nats.MsgMetadata) (*Item, error) {
	item := &Item{}

	if c.consumeAll {
		uid := uuid.NewString()
		c.log.Debug("get raw payload", zap.String("assigned ID", uid))
		item.Job = auto
		item.Ident = fmt.Sprintf("%d:%d", meta.Sequence.Consumer, meta.Sequence.Stream)
		item.Payload = string(m.Data)
		item.Headers = m.Header
		item.Options = &Options{Priority: 10, Pipeline: auto}

		return item, nil
	}

	err := json.Unmarshal(m.Data, item)
	if err != nil {
		return nil, err
	}

	return item, nil
}
