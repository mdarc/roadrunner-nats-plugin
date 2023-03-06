package natsjobs

import (
	"context"
	stderr "errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
)

const (
	pluginName      string = "nats"
	reconnectBuffer int    = 20 * 1024 * 1024
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Driver struct {
	// system
	sync.RWMutex
	log        *zap.Logger
	queue      pq.Queue
	listeners  uint32
	pipeline   atomic.Pointer[jobs.Pipeline]
	consumeAll bool
	stopCh     chan struct{}

	// nats
	conn  *nats.Conn
	sub   *nats.Subscription
	msgCh chan *nats.Msg
	js    nats.JetStreamContext

	// config
	priority           int64
	subject            string
	stream             string
	prefetch           int
	rateLimit          uint64
	deleteAfterAck     bool
	deliverNew         bool
	deliverLast        bool
	deleteStreamOnStop bool
	durable            string
}

func FromConfig(configKey string, log *zap.Logger, cfg Configurer, pipe jobs.Pipeline, pq pq.Queue, _ chan<- jobs.Commander) (*Driver, error) {
	const op = errors.Op("new_nats_consumer")

	log.Info("NATS fromConfig...")
	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nats configuration, global configuration should contain NATS URL"))
	}

	var conf *config
	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefaults()

	conn, err := nats.Connect(conf.Addr, buildNatsOptions(conf, log)...)
	if err != nil {
		return nil, errors.E(op, err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, errors.E(op, err)
	}

	var si *nats.StreamInfo
	si, err = js.StreamInfo(conf.Stream)
	if err != nil {
		if stderr.Is(err, nats.ErrStreamNotFound) {
			si, err = js.AddStream(&nats.StreamConfig{
				Name:     conf.Stream,
				Subjects: []string{conf.Subject},
			})
			if err != nil {
				return nil, errors.E(op, err)
			}
		} else {
			return nil, errors.E(op, err)
		}
	}

	if si == nil {
		return nil, errors.E(op, errors.Str("failed to create a stream"))
	}

	cs := &Driver{
		log:    log,
		stopCh: make(chan struct{}),
		queue:  pq,

		conn:               conn,
		js:                 js,
		priority:           conf.Priority,
		subject:            conf.Subject,
		stream:             conf.Stream,
		consumeAll:         conf.ConsumeAll,
		deleteAfterAck:     conf.DeleteAfterAck,
		deleteStreamOnStop: conf.DeleteStreamOnStop,
		prefetch:           conf.Prefetch,
		deliverNew:         conf.DeliverNew,
		deliverLast:        conf.DeliverLast,
		rateLimit:          conf.RateLimit,
		durable:            conf.Durable,
		msgCh:              make(chan *nats.Msg, conf.Prefetch),
	}

	cs.pipeline.Store(&pipe)

	return cs, nil
}

func FromPipeline(pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq pq.Queue, _ chan<- jobs.Commander) (*Driver, error) {
	log.Info("NATS from Pipeline...")
	const op = errors.Op("new_nats_pipeline_consumer")

	// if no global section -- error
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nats configuration, global configuration should contain NATS URL"))
	}

	var conf *config
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefaults()

	conn, err := nats.Connect(conf.Addr, buildNatsOptions(conf, log)...)
	if err != nil {
		return nil, errors.E(op, err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, errors.E(op, err)
	}

	var si *nats.StreamInfo
	si, err = js.StreamInfo(pipe.String(pipeStream, "default-stream"))
	if err != nil {
		if stderr.Is(err, nats.ErrStreamNotFound) {
			si, err = js.AddStream(&nats.StreamConfig{
				Name:     pipe.String(pipeStream, "default-stream"),
				Subjects: []string{pipe.String(pipeSubject, "default")},
			})
			if err != nil {
				return nil, errors.E(op, err)
			}
		} else {
			return nil, errors.E(op, err)
		}
	}

	if si == nil {
		return nil, errors.E(op, errors.Str("failed to create a stream"))
	}

	cs := &Driver{
		log:    log,
		queue:  pq,
		stopCh: make(chan struct{}),

		conn:               conn,
		js:                 js,
		priority:           pipe.Priority(),
		consumeAll:         pipe.Bool(pipeConsumeAll, false),
		subject:            pipe.String(pipeSubject, "default"),
		stream:             pipe.String(pipeStream, "default-stream"),
		prefetch:           pipe.Int(pipePrefetch, 100),
		deleteAfterAck:     pipe.Bool(pipeDeleteAfterAck, false),
		deliverNew:         pipe.Bool(pipeDeliverNew, false),
		deliverLast:        pipe.Bool(pipeDeliverLast, false),
		deleteStreamOnStop: pipe.Bool(pipeDeleteStreamOnStop, false),
		rateLimit:          uint64(pipe.Int(pipeRateLimit, 1000)),
		durable:            pipe.String(pipeDurable, ""),
		msgCh:              make(chan *nats.Msg, pipe.Int(pipePrefetch, 100)),
	}

	cs.pipeline.Store(&pipe)

	return cs, nil
}

func (c *Driver) Push(_ context.Context, job jobs.Job) error {
	const op = errors.Op("nats_consumer_push")
	if job.Delay() > 0 {
		return errors.E(op, errors.Str("nats doesn't support delayed messages, see: https://github.com/nats-io/nats-streaming-server/issues/324"))
	}

	data, err := json.Marshal(job)
	if err != nil {
		return errors.E(op, err)
	}

	_, err = c.js.Publish(c.subject, data)
	if err != nil {
		return errors.E(op, err)
	}

	job = nil
	return nil
}

func (c *Driver) Register(_ context.Context, p jobs.Pipeline) error {
	c.pipeline.Store(&p)
	return nil
}

func (c *Driver) Run(_ context.Context, p jobs.Pipeline) error {
	start := time.Now()
	const op = errors.Op("nats_run")

	pipe := *c.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		c.log.Warn("nats listener is already in the active state")
		return nil
	}

	atomic.AddUint32(&c.listeners, 1)
	err := c.listenerInit()
	if err != nil {
		return errors.E(op, err)
	}

	c.listenerStart()

	c.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (c *Driver) Pause(_ context.Context, p string) error {
	start := time.Now()

	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	// remove listener
	atomic.AddUint32(&c.listeners, ^uint32(0))

	if c.sub != nil {
		err := c.sub.Drain()
		if err != nil {
			c.log.Error("drain error", zap.Error(err))
		}
	}

	c.stopCh <- struct{}{}
	c.sub = nil

	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (c *Driver) Resume(_ context.Context, p string) error {
	start := time.Now()
	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		return errors.Str("nats listener is already in the active state")
	}

	err := c.listenerInit()
	if err != nil {
		return err
	}

	c.listenerStart()

	atomic.AddUint32(&c.listeners, 1)

	c.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (c *Driver) State(_ context.Context) (*jobs.State, error) {
	pipe := *c.pipeline.Load()

	st := &jobs.State{
		Pipeline: pipe.Name(),
		Priority: uint64(pipe.Priority()),
		Driver:   pipe.Driver(),
		Queue:    c.subject,
		Ready:    ready(atomic.LoadUint32(&c.listeners)),
	}

	if c.sub != nil {
		ci, err := c.sub.ConsumerInfo()
		if err != nil {
			return nil, err
		}

		if ci != nil {
			st.Active = int64(ci.NumAckPending)
			st.Reserved = int64(ci.NumWaiting)
			st.Delayed = 0
		}
	}

	return st, nil
}

func (c *Driver) Stop(_ context.Context) error {
	start := time.Now()

	if atomic.LoadUint32(&c.listeners) > 0 {
		if c.sub != nil {
			err := c.sub.Drain()
			if err != nil {
				c.log.Error("drain error", zap.Error(err))
			}
		}

		c.stopCh <- struct{}{}
	}

	if c.deleteStreamOnStop {
		err := c.js.DeleteStream(c.stream)
		if err != nil {
			return err
		}
	}

	pipe := *c.pipeline.Load()
	err := c.conn.Drain()
	if err != nil {
		return err
	}

	c.conn.Close()
	c.msgCh = nil
	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))

	return nil
}

// private

func (c *Driver) requeue(item *Item) error {
	const op = errors.Op("nats_requeue")
	if item.Options.Delay > 0 {
		return errors.E(op, errors.Str("nats doesn't support delayed messages, see: https://github.com/nats-io/nats-streaming-server/issues/324"))
	}

	data, err := json.Marshal(item)
	if err != nil {
		return errors.E(op, err)
	}

	_, err = c.js.Publish(c.subject, data)
	if err != nil {
		return errors.E(op, err)
	}

	// delete the old message
	_ = c.js.DeleteMsg(c.stream, item.Options.seq)

	item = nil
	return nil
}

func reconnectHandler(log *zap.Logger) func(*nats.Conn) {
	return func(conn *nats.Conn) {
		log.Warn("connection lost, reconnecting", zap.String("url", conn.ConnectedUrl()))
	}
}

func disconnectHandler(log *zap.Logger) func(*nats.Conn, error) {
	return func(_ *nats.Conn, err error) {
		if err != nil {
			log.Error("nats disconnected", zap.Error(err))
			return
		}

		log.Warn("nats disconnected")
	}
}

func ready(r uint32) bool {
	return r > 0
}

func buildNatsOptions(conf *config, log *zap.Logger) []nats.Option {
	natsOptions := []nats.Option{
		nats.Name(conf.Name),
		nats.Token(conf.Token),
		nats.UserInfo(conf.User, conf.Password),
		nats.NoEcho(),
		nats.Timeout(time.Minute),
		nats.MaxReconnects(-1),
		nats.PingInterval(time.Second * 10),
		nats.ReconnectWait(time.Second),
		nats.ReconnectBufSize(reconnectBuffer),
		nats.ReconnectHandler(reconnectHandler(log)),
		nats.DisconnectErrHandler(disconnectHandler(log)),
	}

	return natsOptions
}
