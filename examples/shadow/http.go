package shadow

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/suraciii/gor"
	"github.com/suraciii/gor/examples/shadow/domain"
)

type handler struct {
	runtime *gor.Runtime
}

func NewHandler(rt *gor.Runtime) http.Handler {
	h := &handler{runtime: rt}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /devices/{deviceID}/reports", h.report)
	mux.HandleFunc("PUT /devices/{deviceID}/configuration", h.configure)
	mux.HandleFunc("GET /devices/{deviceID}/shadow", h.shadow)
	mux.HandleFunc("GET /workshops/{workshopID}/online-count", h.onlineCount)
	return mux
}

type reportRequest struct {
	WorkshopID string `json:"workshop_id"`
	State      string `json:"state"`
}

type configurationRequest struct {
	Configuration string `json:"configuration"`
}

type shadowResponse struct {
	ReportedState string    `json:"reported_state"`
	ReportedAt    time.Time `json:"reported_at"`
	Online        bool      `json:"online"`
	WorkshopID    string    `json:"workshop_id"`
	Configuration string    `json:"configuration"`
}

func (h *handler) report(w http.ResponseWriter, r *http.Request) {
	var request reportRequest
	if err := decodeJSON(r, &request); err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	device := gor.Ref[domain.Device](h.runtime, r.PathValue("deviceID"))
	if err := device.Report(r.Context(), request.WorkshopID, request.State); err != nil {
		h.writeError(w, statusForError(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) configure(w http.ResponseWriter, r *http.Request) {
	var request configurationRequest
	if err := decodeJSON(r, &request); err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	device := gor.Ref[domain.Device](h.runtime, r.PathValue("deviceID"))
	if err := device.Configure(r.Context(), request.Configuration); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) shadow(w http.ResponseWriter, r *http.Request) {
	device := gor.Ref[domain.Device](h.runtime, r.PathValue("deviceID"))
	value, err := device.Shadow(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.writeJSON(w, http.StatusOK, shadowResponse{
		ReportedState: value.ReportedState,
		ReportedAt:    value.ReportedAt,
		Online:        value.Online,
		WorkshopID:    value.WorkshopID,
		Configuration: value.Configuration,
	})
}

func (h *handler) onlineCount(w http.ResponseWriter, r *http.Request) {
	workshop := gor.Ref[domain.Workshop](h.runtime, r.PathValue("workshopID"))
	count, err := workshop.OnlineCount(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.writeJSON(w, http.StatusOK, struct {
		OnlineCount int `json:"online_count"`
	}{OnlineCount: count})
}

func decodeJSON(r *http.Request, destination any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode request JSON: %w", err)
	}
	return nil
}

func statusForError(err error) int {
	if errors.Is(err, domain.ErrWorkshopIDRequired) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func (h *handler) writeError(w http.ResponseWriter, status int, err error) {
	h.writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: err.Error()})
}

func (h *handler) writeJSON(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	data = append(data, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		return
	}
}
