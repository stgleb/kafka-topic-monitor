package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	. "kafka-topic-monitor/pkg/logger"
)

// StartHTTPServer creates and starts an HTTP server with a /topics endpoint
func StartHTTPServer(ctx context.Context, listenAddr string, queryChan chan chan []byte) error {
	// Create a new router
	router := mux.NewRouter()
	topicHandler := func(w http.ResponseWriter, r *http.Request) {
		// Set content type
		w.Header().Set("Content-Type", "text/csv")

		query := make(chan []byte)
		queryChan <- query
		report := <-query

		// Return topics as JSON
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(report); err != nil {
			GetLogger().Errorf("error writing report: %v", err)
		}
	}

	// Register routes
	router.HandleFunc("/topics", topicHandler).Methods("GET")

	// Create the server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d"),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Channel to signal server error
	errChan := make(chan error, 1)

	// Start the server in a goroutine
	go func() {
		GetLogger().Infof("Starting HTTP server on port %s", listenAddr)
		if err := server.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			errChan <- fmt.Errorf("error starting server: %w", err)
		}
	}()

	go func() {
		<-ctx.Done()
		// Context was cancelled, shutdown gracefully
		GetLogger().Infof("Shutting down HTTP server...")

		// Create shutdown context with timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			GetLogger().Errorf("error shutting down server: %w", err)
		}

		GetLogger().Errorf("HTTP server shutdown complete")
	}()
	return nil
}
