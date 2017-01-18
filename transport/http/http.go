// Package http is an HTTP-based query transport.
package http

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/tracer/tracer/server"
)

func init() {
	server.RegisterQueryTransport("http", setup)
}

func setup(srv *server.Server, conf map[string]interface{}) (server.QueryTransport, error) {
	listen, ok := conf["listen"].(string)
	if !ok {
		return nil, errors.New("missing listen setting for HTTP transport")
	}
	h := &HTTP{
		srv:    srv,
		listen: listen,
		mux:    http.NewServeMux(),
	}

	h.mux.HandleFunc("/trace", h.TraceByID)
	h.mux.HandleFunc("/span", h.SpanByID)
	h.mux.HandleFunc("/trace/query", h.QueryTraces)

	h.mux.HandleFunc("/services", h.ListServices)

	return h, nil
}

type HTTP struct {
	srv    *server.Server
	listen string
	mux    *http.ServeMux
}

// Start implements the server.QueryTransport interface.
func (h *HTTP) Start() error {
	return http.ListenAndServe(h.listen, h.mux)
}

func (h *HTTP) TraceByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	trace, err := h.srv.Storage.TraceByID(id)
	if err != nil {
		// TODO(dh): handle 404 special
		http.Error(w, err.Error(), 500)
		return
	}
	// TODO(dh): embed error in the JSON
	_ = json.NewEncoder(w).Encode(trace)
}

func (h *HTTP) SpanByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	span, err := h.srv.Storage.SpanByID(id)
	if err != nil {
		// TODO(dh): handle 404 special
		http.Error(w, err.Error(), 500)
		return
	}
	// TODO(dh): embed error in the JSON
	_ = json.NewEncoder(w).Encode(span)
}

func (h *HTTP) ListServices(w http.ResponseWriter, r *http.Request) {
	svcs, err := h.srv.Storage.Services()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	err = json.NewEncoder(w).Encode(svcs)
	if err != nil {
		log.Printf("Error encoding /services response: %+v", err)
	}
}

func (h *HTTP) QueryTraces(w http.ResponseWriter, r *http.Request) {
	args := r.URL.Query()
	var qry server.Query
	qry.Num = 10

	if tmp, ok := args["start_time"]; ok {
		qry.StartTime, _ = time.Parse(time.RFC3339, tmp[0])
	}

	if tmp, ok := args["finish_time"]; ok {
		qry.FinishTime, _ = time.Parse(time.RFC3339, tmp[0])
	}

	if tmp, ok := args["limit"]; ok {
		tmp2, err := strconv.ParseUint(tmp[0], 10, 64)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		qry.Num = int(tmp2)
	}

	if tmp, ok := args["services"]; ok {
		qry.ServiceNames = tmp[:]
	}

	spans, err := h.srv.Storage.QueryTraces(qry)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	err = json.NewEncoder(w).Encode(spans)
	if err != nil {
		log.Printf("Error encoding /trace/query response: %+v", err)
	}
}