package harness

import (
	"net/http"
	"testing"
	"time"

	"github.com/jothishkamal/mnemosyne/sdk"
)

// AcceptanceScenario builds the Spec 1 incident: checkout-api calls
// inventory-svc, which retries pricing-svc three times; the final attempt hangs
// and never returns. pricing-svc runs with a skewed clock.
func AcceptanceScenario(t *testing.T, sink sdk.Sink) *Harness {
	t.Helper()
	var attempts int

	return New(t, Options{
		Sink: sink,
		Services: []ServiceSpec{
			{
				Name: "checkout-api",
				Handler: func(c *Ctx) {
					if _, err := c.Call("inventory-svc", "/stock"); err != nil {
						c.Fail(http.StatusInternalServerError)
						return
					}
					c.Fail(http.StatusInternalServerError)
				},
			},
			{
				Name: "inventory-svc",
				Handler: func(c *Ctx) {
					if err := c.CallWithRetries("pricing-svc", "/price", 3); err != nil {
						c.Fail(http.StatusInternalServerError)
					}
				},
			},
			{
				Name:      "pricing-svc",
				ClockSkew: -400 * time.Millisecond,
				Handler: func(c *Ctx) {
					attempts++
					if attempts >= 3 {
						c.Hang() // the operation that never ends
						return
					}
					c.Fail(http.StatusInternalServerError)
				},
			},
		},
	})
}
