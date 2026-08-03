//go:build sim

package sim

import (
	"fmt"
	"strings"

	"github.com/suraciii/gor/store"
)

type eventLog struct {
	lines        []string
	nextDecision int
}

func (l *eventLog) add(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *eventLog) addDecision(id store.Identity, plan faultPlan, deltas []int64) {
	values := make([]string, len(deltas))
	for index, delta := range deltas {
		values[index] = fmt.Sprintf("%d", delta)
	}
	l.add("%04d decision entity=%s/%s deltas=[%s] fault=%s", l.nextDecision, id.Type, id.Key, strings.Join(values, ","), plan.eventName())
	l.nextDecision++
}

func (l *eventLog) addOutcomes(outcomes []string) {
	l.add("     observe outcomes=[%s]", strings.Join(outcomes, ","))
}

func (l *eventLog) addState(id store.Identity, value int64) {
	l.add("     observe state %s/%s=%d", id.Type, id.Key, value)
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
