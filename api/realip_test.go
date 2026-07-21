package api

import (
	"net/netip"
	"testing"
)

func TestResolveForwardedClientIPIgnoresHeadersFromUntrustedPeer(t *testing.T) {
	trusted, invalid := parseTrustedProxyCIDRs("10.0.0.0/8")
	if len(invalid) != 0 {
		t.Fatalf("unexpected invalid CIDRs: %v", invalid)
	}

	peer := netip.MustParseAddr("203.0.113.10")
	got := resolveForwardedClientIP(peer, []string{"192.0.2.99"}, trusted)
	if got != peer {
		t.Fatalf("untrusted peer spoofed client IP: got %s", got)
	}
}

func TestResolveForwardedClientIPUsesRightmostUntrustedHop(t *testing.T) {
	trusted, _ := parseTrustedProxyCIDRs("10.0.0.0/8, 172.16.0.0/12")
	peer := netip.MustParseAddr("10.0.1.5")

	got := resolveForwardedClientIP(peer, []string{"192.0.2.250, 198.51.100.7, 172.16.2.4"}, trusted)
	if want := netip.MustParseAddr("198.51.100.7"); got != want {
		t.Fatalf("expected actual client %s, got %s", want, got)
	}
}

func TestParseTrustedProxyCIDRsRejectsInvalidValues(t *testing.T) {
	trusted, invalid := parseTrustedProxyCIDRs("10.0.0.0/8, nope, 192.168.0.0/16")
	if len(trusted) != 2 || len(invalid) != 1 || invalid[0] != "nope" {
		t.Fatalf("unexpected parse result: trusted=%v invalid=%v", trusted, invalid)
	}
}
