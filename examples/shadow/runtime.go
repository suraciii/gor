package shadow

import (
	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/examples/shadow/gorgen"
)

func Register(rt *gor.Runtime, sourceClock clock.Clock) error {
	if err := gorgen.Install(rt); err != nil {
		return err
	}
	if err := gor.Register[domain.Device](rt, func(b *gor.Binder) domain.Device {
		return domain.NewDevice(rt, b, sourceClock)
	}); err != nil {
		return err
	}
	return gor.Register[domain.Workshop](rt, domain.NewWorkshop)
}
