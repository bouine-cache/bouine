// Package api exposes the wire-stable types shared between the HTTP
// admin surface, the Go SDK (pkg/bouineapi), and the dashboard. It MUST
// NOT import any internal/* package.
//
// Compatibility rules:
//
//   - Adding fields is a minor bump.
//   - Removing or renaming a field requires a major bump.
//   - Enums are open-ended: clients MUST tolerate unknown values.
package api
