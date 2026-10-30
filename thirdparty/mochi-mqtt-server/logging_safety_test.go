package mqtt

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestHooksOnPublishReasonCodeDoesNotLogPacketPayload(t *testing.T) {
	var buf bytes.Buffer
	h := new(Hooks)
	h.Log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	hook := new(modifiedHookBase)
	hook.fail = true
	hook.err = packets.ErrQuotaExceeded
	require.NoError(t, h.Add(hook, nil))

	payload := []byte("SENTINEL-PAYLOAD-DO-NOT-LOG")
	_, err := h.OnPublish(new(Client), packets.Packet{Payload: payload, PacketID: 10})
	require.ErrorIs(t, err, packets.ErrQuotaExceeded)
	require.Contains(t, buf.String(), "publish packet refused by hook")
	require.Contains(t, buf.String(), "reason=\"quota exceeded\"")
	require.NotContains(t, buf.String(), string(payload))
	require.NotContains(t, buf.String(), "packet=")
}
