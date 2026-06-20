package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
	"os"

	"github.com/gorilla/websocket"
)

type WsEnvelope struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// Connection updater. Defaulted to accept all origins.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true ;
	},
}

// StartWebSocketServer boots the HTTP listener and handles client connections
func StartWebSocketServer(hub *EventHub, port string, dbURL string, dbKey string) {

	// Hydration endpoint during frontend startup
	http.HandleFunc("/api/hydrate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*");

		url := fmt.Sprintf("%s/rest/v1/arbitrage_ledger?select=*&order=timestamp.desc&limit=25", dbURL);
		req, _ := http.NewRequest("GET", url, nil);
		req.Header.Add("apikey", dbKey);
		req.Header.Add("Authorization", "Bearer "+dbKey);

		client := &http.Client{Timeout: 5 * time.Second};
		resp, err := client.Do(req);
		if err != nil {
			http.Error(w, "Failed to fetch ledger from cold storage", http.StatusInternalServerError);
			return;
		}
		defer resp.Body.Close();

		w.Header().Set("Content-Type", "application/json");
		io.Copy(w, resp.Body);
	})

	http.HandleFunc("/api/totals", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*");

		url := fmt.Sprintf("%s/rest/v1/rpc/get_ledger_totals", dbURL);
		jsonBody := []byte(`{}`);
		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody));
		req.Header.Add("apikey", dbKey);
		req.Header.Add("Authorization", "Bearer "+dbKey);
		req.Header.Add("Content-Type", "application/json");

		client := &http.Client{Timeout: 5 * time.Second};
		resp, err := client.Do(req);
		if err != nil {
			http.Error(w, "Failed to fetch ledger totals from cold storage", http.StatusInternalServerError);
			return;
		}
		defer resp.Body.Close();

		w.Header().Set("Content-Type", "application/json");
		io.Copy(w, resp.Body);
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil);
		if err != nil {
			slog.Error("WebSocket upgrade failed", "error", err, "client_ip", r.RemoteAddr);
			return
		}
		defer conn.Close();

		slog.Info("Dashboard connected", "client_ip", r.RemoteAddr);

		arbStream := hub.SubscribeArbitrage();

		// Checking if a client left the connection in which case the conn.Close() is executed
		clientDead := make(chan struct{});
		go func() {
			defer close(clientDead);
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					slog.Info("Dashboard disconnected", "client_ip", r.RemoteAddr);
					return;
				}
			}
		}()

		// Pushes a live TPS/Uptime update every 500 Millisecond
		metricsTicker := time.NewTicker(500 * time.Millisecond)
		defer metricsTicker.Stop()

		// Write Loop
		for {
			select {
			// Client left. Kill the write loop.
			case <-clientDead:
				return

			// Package and send the Arbitrage Cycle
			case arb := <-arbStream:
				envelope := WsEnvelope{
					Type: "ARBITRAGE_FOUND",
					Data: arb,
				}
				if err := conn.WriteJSON(envelope); err != nil {
					return
				}

			// Safely read the global atomic counters
			case <-metricsTicker.C:
				ticks := atomic.LoadUint64(&GlobalTickCount)
				cycleCount := atomic.LoadUint64(&GlobalCycleCount);
				uptime := time.Since(GlobalStartTime).Seconds()
				tps := uint64(float64(ticks) / uptime)

				envelope := WsEnvelope{
					Type: "METRICS_UPDATE",
					Data: MetricsPayload{
						TPS:           tps,
						UptimeSeconds: uptime,
						TotalCycleCount: cycleCount,
					},
				}
				if err := conn.WriteJSON(envelope); err != nil {
					return
				}
			}
		}
	})

	slog.Info("API Gateway live", "port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("HTTP server crashed", "error", err)
		os.Exit(1)
	}
}
