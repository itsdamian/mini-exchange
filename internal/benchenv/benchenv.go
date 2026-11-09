// Package benchenv collects the environment facts every benchmark result must
// record: hardware, OS, Go version, GOMAXPROCS, GOGC, GOMEMLIMIT and commit.
package benchenv

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
)

// Env is the recorded environment of a benchmark run.
type Env struct {
	Commit     string `json:"commit"`
	Hardware   string `json:"hardware"`
	OS         string `json:"os"`
	GoVersion  string `json:"go_version"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	GOGC       string `json:"gogc"`
	GOMEMLIMIT string `json:"gomemlimit"`
}

// Capture reads the current environment.
func Capture() Env {
	return Env{
		Commit: gitCommit(), Hardware: hardware(), OS: runtime.GOOS + "/" + runtime.GOARCH,
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		GOGC: envOr("GOGC", "100 (default)"), GOMEMLIMIT: envOr("GOMEMLIMIT", "unset"),
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func gitCommit() string {
	rev, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" {
					return s.Value
				}
			}
		}
		return "unknown"
	}
	c := strings.TrimSpace(string(rev))
	if st, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(strings.TrimSpace(string(st))) > 0 {
		c += "-dirty"
	}
	return c
}

func hardware() string {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("sysctl", "-n", "machdep.cpu.brand_string")
	case "linux":
		cmd = exec.Command("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
	default:
		return runtime.GOARCH
	}
	b, err := cmd.Output()
	if err != nil {
		return runtime.GOARCH
	}
	return fmt.Sprintf("%s (%d logical CPUs)", strings.TrimSpace(string(b)), runtime.NumCPU())
}
