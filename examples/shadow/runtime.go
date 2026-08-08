package shadow

import (
	"log"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/examples/shadow/domain"
	"github.com/suraciii/gor/examples/shadow/domain/gorgen"
)

func LogBackgroundError(event gor.BackgroundError) {
	switch source := event.Source.(type) {
	case gor.ReminderInvocation:
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

// RegisterConformance installs the Single Silo recovery example with an
// application-owned store. The runtime uses no membership or transport.
func RegisterConformance(rt *gor.Runtime, application domain.ApplicationStore) error {
	return registerConformance(rt, application, nil)
}

// RegisterConformanceWithObservation installs the conformance example and
// sends Reminder observations to observed for deterministic tests.
func RegisterConformanceWithObservation(rt *gor.Runtime, application domain.ApplicationStore, observed chan<- domain.RecoveryObservation) error {
	return registerConformance(rt, application, observed)
}

func registerConformance(rt *gor.Runtime, application domain.ApplicationStore, observed chan<- domain.RecoveryObservation) error {
	if err := register(rt,
		func(b *gor.Binder) domain.Device {
			return domain.NewDeviceWithApplication(b, application)
		},
		domain.NewWorkshop,
	); err != nil {
		return err
	}
	return gor.Register[domain.RecoveryCoordinator](rt, func(b *gor.Binder) domain.RecoveryCoordinator {
		if observed == nil {
			return domain.NewRecoveryCoordinator(b, application)
		}
		return domain.NewRecoveryCoordinatorWithObservation(b, application, observed)
	})
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
