package main

import (
	"net/http"

	"github.com/keel-iot/keel-mqtt-gateway/internal/broker"
)

// newReadyzHandler builds the /readyz handler. reloader is a pointer to the
// (possibly not-yet-assigned) *broker.CertReloader variable so the handler
// always reads its current value, even though it's registered on the mux
// before broker.New runs — see the comment at its call site in runServer.
//
// When tlsEnabled, readiness requires a currently valid, unexpired
// certificate: a missing, unparsable, or expired cert reports NotReady
// rather than silently letting the node serve plain TCP only.
func newReadyzHandler(tlsEnabled bool, reloader **broker.CertReloader) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if tlsEnabled {
			r := *reloader
			if r == nil || !r.Ready() {
				http.Error(w, "tls: certificate not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}
