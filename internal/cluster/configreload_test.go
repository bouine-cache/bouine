package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestConfigReloadCodecRoundTrip(t *testing.T) {
	t.Parallel()
	evt := api.ConfigReloadEvent{
		Gen:          7,
		ConfigSHA256: "0123456789abcdef",
		Issuer:       "node-a",
		Seq:          42,
	}
	body, err := EncodeConfigReloadHTTP(evt)
	require.NoError(t, err)
	out, err := DecodeConfigReloadHTTP(body)
	require.NoError(t, err)
	assert.Equal(t, evt.Gen, out.Gen)
	assert.Equal(t, evt.ConfigSHA256, out.ConfigSHA256)
	assert.Equal(t, evt.Issuer, out.Issuer)

	// unknown fields rejected (additive-only wire safety)
	bad := []byte(`{"gen":7,"config_sha256":"x","future_field":1}`)
	_, err = DecodeConfigReloadHTTP(bad)
	require.Error(t, err)

	// trailing garbage rejected
	_, err = DecodeConfigReloadHTTP(append(append([]byte(nil), body...), []byte(` {"x":1}`)...))
	require.Error(t, err)
}

func TestConfigReloadAckCodec(t *testing.T) {
	t.Parallel()
	ack := api.ConfigReloadAck{Status: "stale_config", Gen: 3}
	b, err := EncodeConfigReloadAck(ack)
	require.NoError(t, err)
	out, err := DecodeConfigReloadAck(b)
	require.NoError(t, err)
	assert.Equal(t, "stale_config", out.Status)
	assert.Equal(t, uint64(3), out.Gen)
}
