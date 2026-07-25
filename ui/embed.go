// Package ui serves the embedded replay interface.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed index.html app.js style.css
var assets embed.FS

// Handler serves the replay UI. Everything is embedded in the binary: the
// collector must work with no network access to anywhere else.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, ".")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
