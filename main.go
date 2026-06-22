package main

import (
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/supabase-community/supabase-go"
)


var (
	GlobalTickCount  uint64
	GlobalCycleCount uint64
	GlobalStartTime  time.Time;
)

const MaxAssets = 64;
const tradingFee = 0.001;

func initLogger() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)
}



func main() {
	GlobalStartTime = time.Now();	


	initLogger();
	slog.Info("Arbitrage Engine Boot Sequence Initiated", "version", "1.0.0");

	dbURL := os.Getenv("SUPABASE_URL");
	dbKey := os.Getenv("SUPABASE_KEY");
	port := os.Getenv("PORT");
	
	dbClient, err := supabase.NewClient(dbURL, dbKey, nil);
	if err != nil {
		slog.Error("Cannot initialize Supabase client", "error", err);
		os.Exit(1)
	}

	historicalCount := FetchHistoricalCount(dbURL, dbKey);
	atomic.StoreUint64(&GlobalCycleCount, historicalCount);
	slog.Info("Ledger state synchronized", "starting_count", historicalCount);
	fmt.Printf("[BOOT] Ledger state synchronized. Starting count: %d\n", historicalCount);

	graph, subscriptions := InitMarketGraph();
	tickBuffer := make(chan TickEvent, 100000);
	rawCycleBuffer := make(chan RawCycleData, 5000);
	hub := NewEventHub();

	dbChan := hub.SubscribeArbitrage()

	var trackedAssets []string
	for i := 0; i < graph.AssetCount; i++ {
		trackedAssets = append(trackedAssets, graph.AssetName[i])
	}
	oracle := NewPricingOracle(trackedAssets)

	time.Sleep(2*time.Second);

	go StartSupabaseWorker(dbChan, dbClient)

	go StartFormatterWorker(graph, rawCycleBuffer, hub, oracle);

	go RunArbitrageEngine(graph, tickBuffer, rawCycleBuffer);

	go StreamMultiplexer(subscriptions, graph, tickBuffer);

	StartWebSocketServer(hub, port, dbURL, dbKey);

}
