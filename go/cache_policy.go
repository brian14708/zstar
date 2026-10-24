package zstar

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ContentCachePolicy governs reuse by private decoded-content caches. The zero
// value allows caching an immutable snapshot when no response policy is supplied.
// Restrictive or stale responses are fetched again instead of retained.
type ContentCachePolicy struct {
	denied  bool
	expires time.Time
}

// NewContentCachePolicy parses Cache-Control for a private cache. Shared-cache
// directives such as s-maxage do not override private max-age.
func NewContentCachePolicy(control string) ContentCachePolicy {
	return responseContentCachePolicy(http.Header{"Cache-Control": {control}}, time.Now())
}

func responseContentCachePolicy(h http.Header, requested time.Time) ContentCachePolicy {
	var policy ContentCachePolicy
	var lifetime time.Duration
	hasLifetime := false
	for _, line := range h.Values("Cache-Control") {
		for directive := range strings.SplitSeq(line, ",") {
			name, value, _ := strings.Cut(strings.TrimSpace(directive), "=")
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "no-store", "no-cache":
				policy.denied = true
			case "max-age":
				seconds, err := strconv.ParseUint(strings.Trim(strings.TrimSpace(value), "\""), 10, 64)
				if hasLifetime || err != nil || seconds > uint64((1<<63-1)/int64(time.Second)) {
					policy.denied = true
					continue
				}
				lifetime, hasLifetime = time.Duration(seconds)*time.Second, true
			}
		}
	}
	now := time.Now()
	date, dateErr := http.ParseTime(h.Get("Date"))
	var age time.Duration
	if value := h.Get("Age"); value != "" {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64((1<<63-1)/int64(time.Second)) {
			policy.denied = true
		} else {
			age = time.Duration(seconds) * time.Second
		}
	}
	delay := max(0, now.Sub(requested))
	if age > time.Duration(1<<63-1)-delay {
		policy.denied = true
	} else {
		age += delay
	}
	if dateErr == nil {
		age = max(age, now.Sub(date))
	}
	if !hasLifetime && h.Get("Expires") != "" {
		expires, err := http.ParseTime(h.Get("Expires"))
		if err != nil {
			policy.denied = true
		} else {
			if dateErr != nil {
				date = now
			}
			lifetime, hasLifetime = max(0, expires.Sub(date)), true
		}
	}
	if hasLifetime {
		policy.expires = now.Add(max(0, lifetime-age))
	}
	for _, vary := range h.Values("Vary") {
		for value := range strings.SplitSeq(vary, ",") {
			if strings.TrimSpace(value) == "*" {
				policy.denied = true
			}
		}
	}
	return policy
}

// AllowsReuse reports whether decoded bytes may be retained and reused now.
func (p ContentCachePolicy) AllowsReuse() bool {
	return !p.denied && (p.expires.IsZero() || time.Now().Before(p.expires))
}

// Observe keeps restrictive directives for the source lifetime, so concurrent
// responses cannot relax admission. Freshness follows the latest valid fetch.
func (p *ContentCachePolicy) Observe(response ContentCachePolicy) {
	response.denied = response.denied || p.denied
	*p = response
}
