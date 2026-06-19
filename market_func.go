package main

import (
	"os"
	"log/slog"
	"fmt"
	"math"
	"sort"
	"strings"
)

func calculateDegreeOfCentrality(info *ExchangeInfo) []SymbolInfo {
	assetDegree := make(map[string]int);
	for _, s := range info.Symbols {
		if s.Status == "TRADING" {
			assetDegree[s.BaseAsset]++;
			assetDegree[s.QuoteAsset]++;
		}
	}

	// Extract and sort the Assets by their power
	type AssetScore struct {
		Name   string
		Degree int
	}
	var assets []AssetScore;
	for name, deg := range assetDegree {
		assets = append(assets, AssetScore{Name: name, Degree: deg});
	}
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].Degree > assets[j].Degree;
	})

	// Make a list of top 32 tickers
	topAssets := make(map[string]bool);
	limit := MaxAssets;
	if len(assets) < limit {
		limit = len(assets);
	}
	for i := 0; i < limit; i++ {
		topAssets[assets[i].Name] = true;
	}

	// The Mesh Filter
	var denseMesh []SymbolInfo
	for _, s := range info.Symbols {
		if s.Status == "TRADING" && topAssets[s.BaseAsset] && topAssets[s.QuoteAsset] {
			denseMesh = append(denseMesh, s);
		}
	}

	return denseMesh;
}

func (g *MarketGraph) GetOrRegisterAsset(ticker string) int {
	id, exists := g.AssetIDs[ticker];
	if !exists {
		if g.AssetCount >= MaxAssets {
			panic("[-]Exceeded Max asset limit!");
		}
		id = g.AssetCount;
		g.AssetIDs[ticker] = id;
		g.AssetName[id] = ticker;
		g.AssetCount++;
	}
	return id;
}

func (g *MarketGraph) WipeState() {
	for i := 0; i < g.AssetCount; i++ {
		for j := 0; j < g.AssetCount; j++ {
			g.Weights[i][j] = math.Inf(1);
			g.Rates[i][j] = 0;
			g.Capacities[i][j] = 0;
		}
	}
}

func InitMarketGraph() (*MarketGraph, []string) {
	g := &MarketGraph{
		AssetIDs: make(map[string]int),
		Routes:   make(map[string]EdgeRoute),
	}

	for i := range MaxAssets {
		for j := range MaxAssets {
			if i == j {
				g.Weights[i][j] = 0;
			} else {
				g.Weights[i][j] = math.Inf(1);
			}
		}
	}

	slog.Info("Fetching and mapping topological interconnections...");
	liveData, err := fetchLiveExchangeInfo();
	if err != nil {
		slog.Error("CRITICAL: Network boot failed", "error", err);
		os.Exit(1);
	}
	bestPairs := calculateDegreeOfCentrality(liveData);
	var stream []string;

	for _, pair := range bestPairs {
		u := g.GetOrRegisterAsset(pair.BaseAsset);
		v := g.GetOrRegisterAsset(pair.QuoteAsset);

		g.Routes[pair.Symbol] = EdgeRoute{
			BaseID:  u,
			QuoteID: v,
		}

		streamName := fmt.Sprintf("%s@bookTicker", strings.ToLower(pair.Symbol));
		stream = append(stream, streamName);
	}

	slog.Info("Matrix mapped", "active_routes", len(g.Routes), "unique_assets", g.AssetCount);
	return g, stream;
}
