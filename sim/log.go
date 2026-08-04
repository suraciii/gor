//go:build sim

package sim

import (
	"fmt"
	"strings"
	"time"

	"github.com/suraciii/gor/store"
)

type eventLog struct {
	lines        []string
	nextDecision int
}

func (l *eventLog) add(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *eventLog) addDecisionEvent(format string, args ...any) {
	l.add("%04d decision %s", l.nextDecision, fmt.Sprintf(format, args...))
	l.nextDecision++
}

func (l *eventLog) addCallDecision(nodes []int, id store.Identity, plan faultPlan, deltas []int64) {
	l.addDecisionEvent("call nodes=[%s] entity=%s/%s deltas=[%s] fault=%s", formatIntList(nodes), id.Type, id.Key, formatInt64List(deltas), plan.eventName())
}

func (l *eventLog) addScheduleDecision(node int, id store.Identity, name string, delay, interval time.Duration, fault scheduleFaultKind) {
	l.addDecisionEvent("schedule node=%d entity=%s/%s name=%s after=%s every=%s fault=%s", node, id.Type, id.Key, name, delay, interval, fault.eventName())
}

func (l *eventLog) addDisarmDecision(node int, id store.Identity, name string) {
	l.addDecisionEvent("disarm node=%d entity=%s/%s name=%s", node, id.Type, id.Key, name)
}

func (l *eventLog) addCrashDecision(node int) {
	l.addDecisionEvent("crash node=%d", node)
}

func (l *eventLog) addRestartDecision(node int) {
	l.addDecisionEvent("restart node=%d", node)
}

func formatInt64List(values []int64) string {
	formatted := make([]string, len(values))
	for index, value := range values {
		formatted[index] = fmt.Sprintf("%d", value)
	}
	return strings.Join(formatted, ",")
}

func formatIntList(values []int) string {
	formatted := make([]string, len(values))
	for index, value := range values {
		formatted[index] = fmt.Sprintf("%d", value)
	}
	return strings.Join(formatted, ",")
}

func (l *eventLog) addOutcomes(outcomes []string) {
	l.add("     observe outcomes=[%s]", strings.Join(outcomes, ","))
}

func (l *eventLog) addState(id store.Identity, value int64) {
	l.add("     observe state %s/%s=%d", id.Type, id.Key, value)
}

func (l *eventLog) addScheduleObservation(stats scheduleStats, deliveries int) {
	l.add("     observe schedules list-calls=%d claim-won=%d claim-lost=%d deliveries=%d list-errors=%d list-delays=%d claim-errors=%d claim-applied-errors=%d", stats.listCalls, stats.claimWon, stats.claimLost, deliveries, stats.listErrors, stats.listDelays, stats.claimErrors, stats.claimAppliedErrors)
}

func (l *eventLog) addScheduleOutcome(operation string, node int, outcome string) {
	l.add("     observe %s node=%d outcome=%s", operation, node, outcome)
}

func (p faultPlan) eventName() string {
	parts := make([]string, 0, 2)
	if p.read.kind != faultNone {
		parts = append(parts, p.read.eventName("read"))
	}
	if p.write.kind != faultNone {
		parts = append(parts, p.write.eventName("write"))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

func (f faultSpec) eventName(operation string) string {
	switch f.kind {
	case faultReadError:
		return fmt.Sprintf("%s.error", operation)
	case faultWriteError:
		return fmt.Sprintf("%s.error", operation)
	case faultWriteAppliedError:
		return fmt.Sprintf("%s.applied-then-error", operation)
	case faultDelay:
		return fmt.Sprintf("delay.%s=%s", operation, f.delay)
	default:
		panic("fault has no event name")
	}
}

func (f scheduleFaultKind) eventName() string {
	switch f {
	case scheduleListError:
		return "schedule.list.error"
	case scheduleListDelay:
		return "schedule.list.delay"
	case scheduleClaimError:
		return "schedule.claim.error"
	case scheduleClaimAppliedError:
		return "schedule.claim.applied-then-error"
	default:
		return "none"
	}
}

func (l eventLog) String() string {
	return strings.Join(l.lines, "\n") + "\n"
}

func decisionLines(log string) string {
	lines := strings.Split(strings.TrimSuffix(log, "\n"), "\n")
	decisions := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) > 1 && fields[1] == "decision" {
			decisions = append(decisions, line)
		}
	}
	return strings.Join(decisions, "\n") + "\n"
}
