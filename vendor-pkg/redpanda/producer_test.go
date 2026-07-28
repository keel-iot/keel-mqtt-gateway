package redpanda

import "testing"

func TestScramMechanism(t *testing.T) {
	tests := []struct {
		name string
		cfg  ProducerConfig
		want string
	}{
		{
			name: "explicit SHA-256",
			cfg:  ProducerConfig{SASLUsername: "u", SASLPassword: "p", SASLMechanism: SASLMechanismScramSHA256},
			want: "SCRAM-SHA-256",
		},
		{
			name: "explicit SHA-512",
			cfg:  ProducerConfig{SASLUsername: "u", SASLPassword: "p", SASLMechanism: SASLMechanismScramSHA512},
			want: "SCRAM-SHA-512",
		},
		{
			name: "default when unset is SHA-512",
			cfg:  ProducerConfig{SASLUsername: "u", SASLPassword: "p"},
			want: "SCRAM-SHA-512",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scramMechanism(tt.cfg).Name()
			if got != tt.want {
				t.Errorf("scramMechanism().Name() = %q, want %q", got, tt.want)
			}
		})
	}
}
