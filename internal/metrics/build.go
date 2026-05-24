package metrics

import (
	"path"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type BuildInfo struct {
	Name      string
	Version   string
	Revision  string
	Time      string
	GoVersion string
}

var buildInfoMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "build_info",
		Help: "Build information; constant 1 with version, revision, time, and go_version labels.",
	},
	[]string{"version", "revision", "time", "go_version"},
)

func ReadBuildInfo() BuildInfo {
	b := BuildInfo{Name: "unknown", Version: "unknown", Revision: "unknown", Time: "unknown", GoVersion: "unknown"}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	b.GoVersion = bi.GoVersion
	if bi.Path != "" {
		b.Name = path.Base(bi.Path)
	}
	if bi.Main.Version != "" {
		b.Version = bi.Main.Version
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			b.Revision = s.Value
		case "vcs.time":
			b.Time = s.Value
		}
	}
	return b
}

func RecordBuildInfo(b BuildInfo) {
	buildInfoMetric.WithLabelValues(b.Version, b.Revision, b.Time, b.GoVersion).Set(1)
}
