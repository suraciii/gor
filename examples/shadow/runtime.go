package shadow

import (
	"log"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/examples/shadow/domain/gorgen"
)

func LogBackgroundError(event gor.BackgroundError) {
	switch source := event.Source.(type) {
	case gor.ScheduledInvocation:
		log.Printf("%s/%s.%s failed: %v", event.GrainId.GrainType, event.GrainId.GrainKey, source.Method, event.Err)
	case gor.Deactivation:
		log.Printf("%s/%s deactivation (%v) failed: %v", event.GrainId.GrainType, event.GrainId.GrainKey, source.Reason, event.Err)
	}
}

func Register(rt *gor.Runtime) error {
	return register(rt, domain.NewDevice, domain.NewWorkshop)
}

func RegisterWithLifecycle(rt *gor.Runtime, events chan<- domain.LifecycleEvent) error {
	return register(rt,
		func(b *gor.Binder) domain.Device {
			return domain.NewDeviceWithLifecycle(b, events)
		},
		func(b *gor.Binder) domain.Workshop {
			return domain.NewWorkshopWithLifecycle(b, events)
		},
	)
}

func register(rt *gor.Runtime, deviceFactory func(*gor.Binder) domain.Device, workshopFactory func(*gor.Binder) domain.Workshop) error {
	if err := gorgen.Install(rt); err != nil {
		return err
	}
	if err := gor.Register[domain.Device](rt, deviceFactory); err != nil {
		return err
	}
	return gor.Register[domain.Workshop](rt, workshopFactory)
}
