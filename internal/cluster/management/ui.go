package management

import (
	"embed"
	"net/http"
)

// uiAssets embeds the basic monitoring dashboard — plain vanilla JS, zero
// build step or CDN dependency, polling this API's own JSON endpoints
// every 3s client-side.
//
//go:embed uiassets/index.html
var uiAssets embed.FS

func (a *API) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	http.ServeFileFS(w, r, uiAssets, "uiassets/index.html")
}
