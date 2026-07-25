package capture

import (
	"encoding/json"
	"strings"
)

// Placeholder replaces every redacted value.
const Placeholder = "[REDACTED]"

// Redact describes what must never leave the process.
//
// Spec 1 matches JSON by key name at any depth rather than by JSON path. Full
// JSON-path matching is deferred; key-name matching is strictly broader, so it
// cannot under-redact relative to a path expression.
type Redact struct {
	Headers  []string
	JSONKeys []string
}

// HeaderValue returns the value to record for a header, and whether it was
// redacted. Matching is case-insensitive.
func (r Redact) HeaderValue(name, value string) (string, bool) {
	lower := strings.ToLower(name)
	for _, h := range r.Headers {
		if strings.ToLower(h) == lower {
			return Placeholder, true
		}
	}
	return value, false
}

// Body returns the body to record, and whether anything was redacted. Bodies
// that are not valid JSON are passed through unchanged: Spec 1 only knows how
// to redact JSON, and silently mangling an unknown format would be worse than
// leaving it alone.
func (r Redact) Body(body []byte) ([]byte, bool) {
	if len(r.JSONKeys) == 0 || len(body) == 0 {
		return body, false
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return body, false
	}
	deny := make(map[string]struct{}, len(r.JSONKeys))
	for _, k := range r.JSONKeys {
		deny[strings.ToLower(k)] = struct{}{}
	}
	scrubbed, changed := scrub(v, deny)
	if !changed {
		return body, false
	}
	out, err := json.Marshal(scrubbed)
	if err != nil {
		// Cannot re-serialise safely; drop the body entirely rather than risk
		// emitting the original.
		return []byte(Placeholder), true
	}
	return out, true
}

func scrub(v any, deny map[string]struct{}) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		changed := false
		for k, val := range t {
			if _, bad := deny[strings.ToLower(k)]; bad {
				t[k] = Placeholder
				changed = true
				continue
			}
			sub, c := scrub(val, deny)
			t[k] = sub
			changed = changed || c
		}
		return t, changed
	case []any:
		changed := false
		for i, val := range t {
			sub, c := scrub(val, deny)
			t[i] = sub
			changed = changed || c
		}
		return t, changed
	default:
		return v, false
	}
}
