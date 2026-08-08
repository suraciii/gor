package shadow_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/clock"
	shadow "github.com/suraciii/gor/examples/shadow"
	"github.com/suraciii/gor/store"
)

func TestHTTPReportsConfiguresAndReadsShadow(t *testing.T) {
	sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
	rt, err := gor.New(
		gor.WithStore(store.NewMemory()),
		gor.WithClock(sourceClock),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
		gor.WithReminderInterval(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.Register(rt); err != nil {
		rt.Close()
		t.Fatal(err)
	}
	defer rt.Close()
	handler := shadow.NewHandler(rt)

	report := serve(handler, http.MethodPost, "/devices/device-1/reports", `{"workshop_id":"assembly","state":"temperature=20"}`)
	if report.Code != http.StatusNoContent {
		t.Fatalf("report status = %d, want %d", report.Code, http.StatusNoContent)
	}

	configure := serve(handler, http.MethodPut, "/devices/device-1/configuration", `{"configuration":"sample-rate=10s"}`)
	if configure.Code != http.StatusNoContent {
		t.Fatalf("configure status = %d, want %d", configure.Code, http.StatusNoContent)
	}

	read := serve(handler, http.MethodGet, "/devices/device-1/shadow", "")
	if read.Code != http.StatusOK {
		t.Fatalf("shadow status = %d, want %d", read.Code, http.StatusOK)
	}
	var shadowValue struct {
		ReportedState string    `json:"reported_state"`
		ReportedAt    time.Time `json:"reported_at"`
		Online        bool      `json:"online"`
		WorkshopID    string    `json:"workshop_id"`
		Configuration string    `json:"configuration"`
	}
	if err := json.NewDecoder(read.Body).Decode(&shadowValue); err != nil {
		t.Fatal(err)
	}
	if shadowValue.ReportedState != "temperature=20" || !shadowValue.ReportedAt.Equal(sourceClock.Now()) || !shadowValue.Online || shadowValue.WorkshopID != "assembly" || shadowValue.Configuration != "sample-rate=10s" {
		t.Fatalf("shadow response = %#v, want reported state, time, online, assembly, and configuration", shadowValue)
	}

	count := serve(handler, http.MethodGet, "/workshops/assembly/online-count", "")
	if count.Code != http.StatusOK {
		t.Fatalf("online count status = %d, want %d", count.Code, http.StatusOK)
	}
	var countValue struct {
		OnlineCount int `json:"online_count"`
	}
	if err := json.NewDecoder(count.Body).Decode(&countValue); err != nil {
		t.Fatal(err)
	}
	if countValue.OnlineCount != 1 {
		t.Fatalf("online count response = %d, want 1", countValue.OnlineCount)
	}
}

func TestHTTPRejectsMalformedJSON(t *testing.T) {
	sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
	rt, err := gor.New(gor.WithStore(store.NewMemory()), gor.WithClock(sourceClock), gor.WithReminderInterval(time.Second), gor.WithIdleTimeout(0), gor.WithEvictionInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.Register(rt); err != nil {
		rt.Close()
		t.Fatal(err)
	}
	defer rt.Close()

	response := serve(shadow.NewHandler(rt), http.MethodPost, "/devices/device-1/reports", `{not-json}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("malformed JSON status = %d, want %d", response.Code, http.StatusBadRequest)
	}

	emptyWorkshop := serve(shadow.NewHandler(rt), http.MethodPost, "/devices/device-1/reports", `{"workshop_id":"","state":"temperature=20"}`)
	if emptyWorkshop.Code != http.StatusBadRequest {
		t.Fatalf("empty workshop status = %d, want %d", emptyWorkshop.Code, http.StatusBadRequest)
	}
}

func TestHTTPReportWriteFailureIsNotBadRequest(t *testing.T) {
	backend := &failingWorkshopStore{Memory: store.NewMemory()}
	sourceClock := clock.NewFake(time.Unix(0, 0).UTC())
	rt, err := gor.New(
		gor.WithStore(backend),
		gor.WithClock(sourceClock),
		gor.WithReminderInterval(time.Second),
		gor.WithIdleTimeout(0),
		gor.WithEvictionInterval(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.Register(rt); err != nil {
		rt.Close()
		t.Fatal(err)
	}
	defer rt.Close()
	backend.failWorkshopWrites.Store(true)

	response := serve(shadow.NewHandler(rt), http.MethodPost, "/devices/device-1/reports", `{"workshop_id":"assembly","state":"temperature=20"}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("report status with workshop write failure = %d, want %d", response.Code, http.StatusInternalServerError)
	}
}

func serve(handler http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
