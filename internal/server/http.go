package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(address string) *http.Server {
	httpServer := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", httpServer.handleProduce).Methods("POST")
	router.HandleFunc("/", httpServer.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    address,
		Handler: router,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	decodeError := json.NewDecoder(r.Body).Decode(&req)
	if decodeError != nil {
		http.Error(w, decodeError.Error(), http.StatusBadRequest)
		return
	}
	off, appendError := s.Log.Append(req.Record)
	if appendError != nil {
		http.Error(w, appendError.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	encodeError := json.NewEncoder(w).Encode(res)
	if encodeError != nil {
		http.Error(w, encodeError.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	decodeError := json.NewDecoder(r.Body).Decode(&req)
	if decodeError != nil {
		http.Error(w, decodeError.Error(), http.StatusBadRequest)
		return
	}
	record, offsetError := s.Log.Read(req.Offset)
	if offsetError == ErrOffsetNotFound {
		http.Error(w, offsetError.Error(), http.StatusNotFound)
		return
	}
	if offsetError != nil {
		http.Error(w, offsetError.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	encodeError := json.NewEncoder(w).Encode(res)
	if encodeError != nil {
		http.Error(w, encodeError.Error(), http.StatusInternalServerError)
		return
	}
}
