package api

import (
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"

	"streaming/logger"
)

const trustedProxyCIDRsEnv = "TRUSTED_PROXY_CIDRS"

type trustedProxySet []netip.Prefix

func trustedRealIPMiddleware(log *logger.MultiLogger) func(http.Handler) http.Handler {
	trusted, invalid := parseTrustedProxyCIDRs(os.Getenv(trustedProxyCIDRsEnv))
	for _, value := range invalid {
		log.Warn("ignoring invalid trusted proxy CIDR", "cidr", value)
	}
	if len(trusted) == 0 {
		log.Warn("no trusted proxy CIDRs configured; forwarded client IP headers will be ignored")
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			peer, ok := parseRemoteIP(r.RemoteAddr)
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			client := resolveForwardedClientIP(peer, r.Header.Values("X-Forwarded-For"), trusted)
			r.RemoteAddr = net.JoinHostPort(client.String(), "0")
			next.ServeHTTP(w, r)
		})
	}
}

func parseTrustedProxyCIDRs(value string) (trustedProxySet, []string) {
	var trusted trustedProxySet
	var invalid []string
	for _, raw := range strings.Split(value, ",") {
		candidate := strings.TrimSpace(raw)
		if candidate == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(candidate)
		if err != nil {
			invalid = append(invalid, candidate)
			continue
		}
		trusted = append(trusted, prefix.Masked())
	}
	return trusted, invalid
}

func resolveForwardedClientIP(peer netip.Addr, forwardedHeaders []string, trusted trustedProxySet) netip.Addr {
	peer = peer.Unmap()
	if !trusted.Contains(peer) {
		return peer
	}

	forwarded := strings.Join(forwardedHeaders, ",")
	chain := strings.Split(forwarded, ",")
	for index := len(chain) - 1; index >= 0; index-- {
		candidate, err := netip.ParseAddr(strings.TrimSpace(chain[index]))
		if err != nil {
			continue
		}
		candidate = candidate.Unmap()
		if !trusted.Contains(candidate) {
			return candidate
		}
	}
	return peer
}

func (trusted trustedProxySet) Contains(address netip.Addr) bool {
	address = address.Unmap()
	for _, prefix := range trusted {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func parseRemoteIP(remoteAddress string) (netip.Addr, bool) {
	remoteAddress = strings.TrimSpace(remoteAddress)
	if host, _, err := net.SplitHostPort(remoteAddress); err == nil {
		remoteAddress = host
	}
	address, err := netip.ParseAddr(remoteAddress)
	if err != nil {
		return netip.Addr{}, false
	}
	return address.Unmap(), true
}
