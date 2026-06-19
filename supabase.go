package main

import (
	"log/slog"
	"fmt"
	"time"
	"net/http"

	"github.com/supabase-community/supabase-go"
)

func FetchHistoricalCount(projectURL, apiKey string) uint64 {
	url := fmt.Sprintf("%s/rest/v1/arbitrage_ledger?select=id", projectURL);

	req, _ := http.NewRequest("HEAD", url, nil);
	req.Header.Add("apikey", apiKey);
	req.Header.Add("Authorization", "Bearer "+apiKey);
	
	req.Header.Add("Prefer", "count=exact");

	client := &http.Client{};
	resp, err := client.Do(req);
	if err != nil {
		slog.Warn("Could not fetch historical count from database", "error", err);
		return 0;
	}
	defer resp.Body.Close();

	contentRange := resp.Header.Get("Content-Range");
	if contentRange != "" {
		var start, end, total uint64;
		fmt.Sscanf(contentRange, "%d-%d/%d", &start, &end, &total);
		return total;
	}

	return 0;
}

func StartSupabaseWorker(arbStream <-chan ArbitragePayload, dbClient *supabase.Client) {
	slog.Info("Supabase batch worker initialized");
	
	var batch []ArbitragePayload;
	
	flushTimer := time.NewTicker(5 * time.Second);
	defer flushTimer.Stop();

	for {
		select {
		case arb := <-arbStream:
			batch = append(batch, arb);

		case <-flushTimer.C:
			if len(batch) > 0 {
				uploadToSupabase(dbClient, batch);
				batch = batch[:0];
			}
		}
	}
}

func uploadToSupabase(dbClient *supabase.Client, data []ArbitragePayload) {
	_, _, err := dbClient.From("arbitrage_ledger").Insert(data, false, "", "", "exact").Execute();
	
	if err != nil {
		slog.Error("Failed to flush bulk insert to Supabase", "error", err, "batch_size", len(data));
		return;
	}
	slog.Info("Successfully bulk-inserted cycles to cold ledger", "count", len(data));
}
