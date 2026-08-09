package cmd

import _ "embed"

//go:embed embedded/values.schema.json
var helmSchemaJSON []byte
