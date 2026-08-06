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

func (l *eventLog) addScheduleDecision(node int, id store.Identity, name string, delay, interval time.Duration, fault scheduleFaultSpec, member ...memberFaultSpec) {
	l.addDecisionEvent("schedule node=%d entity=%s/%s name=%s after=%s every=%s fault=%s%s", node, id.Type, id.Key, name, delay, interval, fault.eventName(), memberFaultSuffix(member))
}

func (l *eventLog) addDisarmDecision(node int, id store.Identity, name string, member ...memberFaultSpec) {
	l.addDecisionEvent("disarm node=%d entity=%s/%s name=%s%s", node, id.Type, id.Key, name, memberFaultSuffix(member))
}

func (l *eventLog) addCrashDecision(node int, member ...memberFaultSpec) {
	l.addDecisionEvent("crash node=%d%s", node, memberFaultSuffix(member))
}

func (l *eventLog) addRestartDecision(node int, member ...memberFaultSpec) {
	l.addDecisionEvent("restart node=%d%s", node, memberFaultSuffix(member))
}

func (l *eventLog) addLeaveDecision(node int, member memberFaultSpec) {
	l.addDecisionEvent("leave node=%d%s", node, memberFaultSuffix([]memberFaultSpec{member}))
}

func (l *eventLog) addClusterOutcome(operation string, node int, outcome string) {
	l.add("     observe %s node=%d outcome=%s", operation, node, outcome)
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

func (l *eventLog) addMemberObservation(stats memberStats) {
	l.add("     observe members list-calls=%d write-calls=%d list-errors=%d cas-errors=%d cas-applied-errors=%d delays=%d dead-writes=%d delayed-dead-cas=%d", stats.listCalls, stats.writeCalls, stats.listErrors, stats.casConflicts, stats.casAppliedErrors, stats.delays, stats.deadWrites, stats.delayedDeadCAS)
}

func (l *eventLog) addNetworkDecision(delay time.Duration) {
	l.addDecisionEvent("network-delay=%s", delay)
}

func (l *eventLog) addNetworkObservation(sends, delivered, dropped, held, completed int64) {
	l.add("     observe network sends=%d delivered=%d dropped=%d held=%d completed=%d", sends, delivered, dropped, held, completed)
}

func (l *eventLog) addScheduleOutcome(operation string, node int, outcome string) {
	l.add("     observe %s node=%d outcome=%s", operation, node, outcome)
}

func (p faultPlan) eventName() string {
	parts := make([]string, 0, 3)
	if p.read.kind != faultNone {
		parts = append(parts, p.read.eventName("read"))
	}
	if p.write.kind != faultNone {
		parts = append(parts, p.write.eventName("write"))
	}
	if p.member.kind != memberFaultNone {
		parts = append(parts, p.member.eventName())
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

func (f memberFaultSpec) eventName() string {
	switch f.kind {
	case memberListError:
		return "member.list.error@" + f.target.addr
	case memberCASFailure:
		return "member.cas.error@" + f.target.row.eventName()
	case memberCASAppliedError:
		return "member.cas.applied-then-error@" + f.target.row.eventName()
	case memberDelay:
		return fmt.Sprintf("member.delay=%s@%s", f.delay, f.target.row.eventName())
	default:
		return "none"
	}
}

func (k fakeMemberKey) eventName() string {
	return k.nodeAddr + "/" + k.generation
}

func memberFaultSuffix(fault []memberFaultSpec) string {
	if len(fault) == 0 {
		return ""
	}
	return " member-fault=" + fault[0].eventName()
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

func (f scheduleFaultSpec) eventName() string {
	switch f.kind {
	case scheduleListError:
		return "schedule.list.error@" + nodeAddress(f.targetNode)
	case scheduleListDelay:
		return "schedule.list.delay@" + nodeAddress(f.targetNode)
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
