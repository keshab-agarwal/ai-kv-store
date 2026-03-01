package harness

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type CommandOrchestrator struct {
	StartCmd   string
	StopCmd    string
	CrashCmd   string
	RecoverCmd string
	Env        []string
}

func (o CommandOrchestrator) run(template string, node int) error {
	if strings.TrimSpace(template) == "" {
		return nil
	}
	cmdText := strings.ReplaceAll(template, "{node}", fmt.Sprintf("%d", node))
	cmd := exec.Command("zsh", "-lc", cmdText)
	cmd.Env = append(os.Environ(), o.Env...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cmd failed: %s: %w: %s", cmdText, err, out.String())
	}
	return nil
}

func (o CommandOrchestrator) StartCluster() error {
	return o.run(o.StartCmd, -1)
}

func (o CommandOrchestrator) StopCluster() error {
	return o.run(o.StopCmd, -1)
}

func (o CommandOrchestrator) CrashNode(node int) error {
	return o.run(o.CrashCmd, node)
}

func (o CommandOrchestrator) RecoverNode(node int) error {
	return o.run(o.RecoverCmd, node)
}
