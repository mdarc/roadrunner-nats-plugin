package nats

import (
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/nats/v4/natsjobs"
	"go.uber.org/zap"
)

const pluginName string = "nats"

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Plugin struct {
	log *zap.Logger
	cfg Configurer
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(pluginName) {
		return errors.E(errors.Disabled)
	}

	p.log = log.NamedLogger(pluginName)
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) DriverFromConfig(configKey string, pq pq.Queue, pipeline jobs.Pipeline, cmder chan<- jobs.Commander) (jobs.Driver, error) {
	return natsjobs.FromConfig(configKey, p.log, p.cfg, pipeline, pq, cmder)
}

func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq pq.Queue, cmder chan<- jobs.Commander) (jobs.Driver, error) {
	return natsjobs.FromPipeline(pipe, p.log, p.cfg, pq, cmder)
}
