// Package dashboard provides static assets embedded into the binary.
// Currently there are no static files — the dashboard loads CSS via the
// inline Styles() templ component and scripts from CDN.
// This file satisfies the §6.2 directory layout in PLAN.md and can be
// extended with embed.FS directives for offline-capable deployments.
package dashboard
