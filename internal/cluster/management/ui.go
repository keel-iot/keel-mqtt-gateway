package management

import (
	"embed"
	"net/http"
)

// uiAssets embeds the basic monitoring dashboard (see keel-design-doc.md's
// "Osservabilità e controllo" section, "UI minimale: dashboard HTML+HTMX
// servita direttamente dal binario Go" — this is plain vanilla JS instead
// of HTMX, but the same intent: zero build step, zero external/CDN
// dependency, single static page polling the JSON endpoints already on
// this API). Polls GET /api/cluster/nodes, GET /api/metrics, GET
// /api/cluster/routes, and GET /api/live/clients every 3s client-side.
//
//go:embed uiassets/index.html
var uiAssets embed.FS

func (a *API) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	http.ServeFileFS(w, r, uiAssets, "uiassets/index.html")
}
