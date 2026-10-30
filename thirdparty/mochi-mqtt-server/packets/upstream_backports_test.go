// SPDX-License-Identifier: MIT
// Regression coverage for the validation changes in upstream PR #527.

package packets

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUpstreamConnectDistinguishesAbsentAndZeroLimits(t *testing.T) {
	for _, tc := range []struct {
		name       string
		properties []byte
		want       byte
	}{
		{"absent", nil, 0},
		{"zero-receive-maximum", []byte{33, 0, 0}, 0x82},
		{"positive-receive-maximum", []byte{33, 0, 1}, 0},
		{"zero-maximum-packet-size", []byte{39, 0, 0, 0, 0}, 0x82},
		{"positive-maximum-packet-size", []byte{39, 0, 0, 0, 1}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 60, byte(len(tc.properties))}
			body = append(body, tc.properties...)
			body = append(body, 0, 1, 'x')
			pk := Packet{FixedHeader: FixedHeader{Type: Connect}}
			require.NoError(t, pk.ConnectDecode(body))
			require.Equal(t, tc.want, pk.ConnectValidate().Code)
		})
	}
}

func TestUpstreamSubscribeRejectsExplicitZeroIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name       string
		properties []byte
		want       byte
	}{
		{"absent", nil, 0}, {"zero", []byte{11, 0}, 0x82}, {"positive", []byte{11, 1}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte{0, 1, byte(len(tc.properties))}
			body = append(body, tc.properties...)
			body = append(body, 0, 3, 'a', '/', 'b', 0)
			pk := Packet{FixedHeader: FixedHeader{Type: Subscribe, Qos: 1}, ProtocolVersion: 5}
			require.NoError(t, pk.SubscribeDecode(body))
			require.Equal(t, tc.want, pk.SubscribeValidate().Code)
		})
	}
}
