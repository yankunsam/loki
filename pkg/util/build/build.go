package build

import (
	"encoding/json"
	"net/http"
	"runtime"

	"github.com/prometheus/common/version"
	prom "github.com/prometheus/prometheus/web/api/v1"
)

// Version information passed to Prometheus version package.
// Package path as used by linker changes based on vendoring being used or not,
// so it's easier just to use stable Loki path, and pass it to
// Prometheus in the code.
var (
	Version   string
	Revision  string
	Branch    string
	BuildUser string
	BuildDate string
	GoVersion string
)

func init() {
	version.Version = Version
	version.Revision = Revision
	version.Branch = Branch
	version.BuildUser = BuildUser
	version.BuildDate = BuildDate
	version.GoVersion = runtime.Version()
}

func GetVersion() prom.PrometheusVersion {
	return prom.PrometheusVersion{
		Version:   version.Version,
		Revision:  version.Revision,
		Branch:    version.Branch,
		BuildUser: version.BuildUser,
		BuildDate: version.BuildDate,
		GoVersion: version.GoVersion,
	}
}

func VersionHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// We ignore errors here, because we cannot do anything about them.
		// Write will trigger sending Status code, so we cannot send a different status code afterwards.
		// Also this isn't internal error, but error communicating with client.
		_ = json.NewEncoder(w).Encode(GetVersion())
	}
}
