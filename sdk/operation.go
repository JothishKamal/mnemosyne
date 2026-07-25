package sdk

import v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"

// Operation describes what is being recorded. Implementations populate the
// operation-specific fields of an event.
type Operation interface {
	applyTo(*v1.Event)
}

// HTTPServer records an inbound HTTP request.
type HTTPServer struct {
	Method string
	Route  string
	Path   string
	Status uint32
}

func (o HTTPServer) applyTo(e *v1.Event) {
	e.OperationType = v1.OperationType_HTTP_SERVER
	e.Operation = &v1.Event_HttpServer{HttpServer: &v1.HttpServer{
		Method: o.Method, Route: o.Route, Path: o.Path, Status: o.Status,
	}}
}

// HTTPClient records an outbound HTTP request.
type HTTPClient struct {
	Method string
	Host   string
	Path   string
	Status uint32
}

func (o HTTPClient) applyTo(e *v1.Event) {
	e.OperationType = v1.OperationType_HTTP_CLIENT
	e.Operation = &v1.Event_HttpClient{HttpClient: &v1.HttpClient{
		Method: o.Method, Host: o.Host, Path: o.Path, Status: o.Status,
	}}
}

// Local records in-process work with no external dependency.
type Local struct {
	Name string
}

func (o Local) applyTo(e *v1.Event) {
	e.OperationType = v1.OperationType_LOCAL
	if o.Name != "" {
		if e.Attributes == nil {
			e.Attributes = map[string]*v1.Value{}
		}
		e.Attributes["operation.name"] = &v1.Value{Kind: &v1.Value_StringValue{StringValue: o.Name}}
	}
}
