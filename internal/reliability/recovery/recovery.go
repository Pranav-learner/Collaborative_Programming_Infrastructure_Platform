package recovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cpip/internal/reliability/events"
	"cpip/internal/reliability/metrics"
)

// RecoveryStep defines an action in the restoration workflow.
type RecoveryStep struct {
	Name         string
	Action       func(context.Context) error
	Dependencies []string
}

// RecoveryPlan encapsulates the sequence of actions to recover services.
type RecoveryPlan struct {
	ID    string
	Name  string
	Steps []RecoveryStep
}

// StepResult stores execution metadata.
type StepResult struct {
	StepName string        `json:"step_name"`
	Success  bool          `json:"success"`
	Error    error         `json:"-"`
	ErrorMsg string        `json:"error,omitempty"`
	Duration time.Duration `json:"duration"`
}

// RecoveryReport tracks disaster recovery execution outcomes.
type RecoveryReport struct {
	PlanID      string        `json:"plan_id"`
	PlanName    string        `json:"plan_name"`
	StartTime   time.Time     `json:"start_time"`
	Duration    time.Duration `json:"duration"`
	Success     bool          `json:"success"`
	StepResults []StepResult  `json:"step_results"`
}

// DisasterRecoveryPlanner executes steps in topological order.
type DisasterRecoveryPlanner struct {
	bus     *events.Bus
	metrics metrics.Recorder
}

func NewDisasterRecoveryPlanner(bus *events.Bus, rec metrics.Recorder) *DisasterRecoveryPlanner {
	return &DisasterRecoveryPlanner{
		bus:     bus,
		metrics: rec,
	}
}

// Execute runs the plan after verifying dependency graph sorting.
func (dr *DisasterRecoveryPlanner) Execute(ctx context.Context, plan RecoveryPlan) (RecoveryReport, error) {
	if dr.metrics != nil {
		dr.metrics.Inc(metrics.MetricRecoveryRuns)
	}

	start := time.Now()
	report := RecoveryReport{
		PlanID:      plan.ID,
		PlanName:    plan.Name,
		StartTime:   start,
		StepResults: make([]StepResult, 0),
		Success:     true,
	}

	if dr.bus != nil {
		dr.bus.Publish(events.Event{
			Type:      events.RecoveryStarted,
			Timestamp: start,
			Detail:    fmt.Sprintf("Disaster recovery plan %q started", plan.Name),
		})
	}

	// 1. Sort steps topologically
	sortedSteps, err := sortSteps(plan.Steps)
	if err != nil {
		report.Success = false
		report.Duration = time.Since(start)
		if dr.metrics != nil {
			dr.metrics.Inc(metrics.MetricRecoveryFailures)
		}
		return report, fmt.Errorf("invalid plan topology: %w", err)
	}

	// 2. Execute steps sequentially based on order
	completedSteps := make(map[string]bool)

	for _, step := range sortedSteps {
		// Verify dependencies are met
		for _, dep := range step.Dependencies {
			if !completedSteps[dep] {
				errDep := fmt.Errorf("dependency %q not completed for step %q", dep, step.Name)
				report.StepResults = append(report.StepResults, StepResult{
					StepName: step.Name,
					Success:  false,
					Error:    errDep,
					ErrorMsg: errDep.Error(),
				})
				report.Success = false
				break
			}
		}

		if !report.Success {
			break
		}

		stepStart := time.Now()
		stepErr := step.Action(ctx)
		stepDur := time.Since(stepStart)

		res := StepResult{
			StepName: step.Name,
			Success:  stepErr == nil,
			Error:    stepErr,
			Duration: stepDur,
		}
		if stepErr != nil {
			res.ErrorMsg = stepErr.Error()
			report.Success = false
		} else {
			completedSteps[step.Name] = true
		}

		report.StepResults = append(report.StepResults, res)

		if stepErr != nil {
			break // Halt execution on first failure
		}
	}

	report.Duration = time.Since(start)

	if !report.Success && dr.metrics != nil {
		dr.metrics.Inc(metrics.MetricRecoveryFailures)
	}

	if dr.bus != nil {
		dr.bus.Publish(events.Event{
			Type:      events.RecoveryCompleted,
			Timestamp: time.Now(),
			Detail:    fmt.Sprintf("Disaster recovery plan %q finished. Success: %t. Duration: %v", plan.Name, report.Success, report.Duration),
		})
	}

	return report, nil
}

// sortSteps performs a topological sort on recovery steps.
func sortSteps(steps []RecoveryStep) ([]RecoveryStep, error) {
	adj := make(map[string][]string)
	inDegree := make(map[string]int)
	stepsMap := make(map[string]RecoveryStep)

	for _, s := range steps {
		inDegree[s.Name] = 0
		stepsMap[s.Name] = s
	}

	for _, s := range steps {
		for _, dep := range s.Dependencies {
			if _, exists := stepsMap[dep]; !exists {
				return nil, fmt.Errorf("step %q refers to missing dependency %q", s.Name, dep)
			}
			adj[dep] = append(adj[dep], s.Name)
			inDegree[s.Name]++
		}
	}

	queue := make([]string, 0)
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var order []RecoveryStep
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		order = append(order, stepsMap[curr])

		for _, neighbor := range adj[curr] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(order) != len(steps) {
		return nil, errors.New("cycle detected in disaster recovery plan dependencies")
	}

	return order, nil
}
