package correctness

import (
	"log"
	"math/rand"
	"time"
)

type FaultSchedule struct {
	Events []FaultEvent
}

type FaultEvent struct {
	AtSecond    float64
	Action      string // "crash" or "recover"
	NodeID      uint64
}

func GenerateFaultSchedule(seed int64, totalDuration time.Duration, nodeCount int) FaultSchedule {
	rng := rand.New(rand.NewSource(seed))
	secs := totalDuration.Seconds()

	var events []FaultEvent

	crashTime := 2.0 + rng.Float64()*(secs*0.4)
	targetNode := uint64(rng.Intn(nodeCount) + 1)

	recoveryDelay := 5.0 + rng.Float64()*15.0
	recoverTime := crashTime + recoveryDelay
	if recoverTime > secs-2.0 {
		recoverTime = secs - 2.0
	}

	events = append(events,
		FaultEvent{AtSecond: crashTime, Action: "crash", NodeID: targetNode},
		FaultEvent{AtSecond: recoverTime, Action: "recover", NodeID: targetNode},
	)

	return FaultSchedule{Events: events}
}

type FaultInjector struct {
	orch     *Orchestrator
	schedule FaultSchedule
	logger   *log.Logger
	faultLog []FaultEvent
}

func NewFaultInjector(orch *Orchestrator, schedule FaultSchedule, logger *log.Logger) *FaultInjector {
	return &FaultInjector{
		orch:     orch,
		schedule: schedule,
		logger:   logger,
	}
}

func (f *FaultInjector) Run(startTime time.Time) {
	for _, ev := range f.schedule.Events {
		targetTime := startTime.Add(time.Duration(ev.AtSecond * float64(time.Second)))
		sleepDur := time.Until(targetTime)
		if sleepDur > 0 {
			time.Sleep(sleepDur)
		}

		switch ev.Action {
		case "crash":
			f.logger.Printf("FAULT: crashing node %d at t=%.1fs", ev.NodeID, ev.AtSecond)
			f.orch.CrashNode(ev.NodeID)
		case "recover":
			f.logger.Printf("FAULT: recovering node %d at t=%.1fs", ev.NodeID, ev.AtSecond)
			f.orch.RecoverNode(ev.NodeID)
		}

		f.faultLog = append(f.faultLog, ev)
	}
}

func (f *FaultInjector) FaultCount() int {
	return len(f.faultLog)
}
