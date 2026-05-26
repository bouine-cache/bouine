// Package templates contains the templ components for the operator dashboard.
// Each .templ file compiles to a _templ.go file; the generated files are
// committed alongside the sources so the binary can be built without the
// templ CLI.
//
// Data models consumed by the components are defined in models.go.
//
// To regenerate after editing a .templ file:
//
//	templ generate
//
//go:generate templ generate
package templates
