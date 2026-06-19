package engine

import (
	"math"
	"sync/atomic"
	"strings"
	"time"
	"fmt"
)

type RawCycleData struct {
	Path   		 []int;
	Multiplier float64;
	Capacity   float64;
}

type SPFAState struct {
	Dist    [MaxAssets]float64;
	Parent  [MaxAssets]int;
	Visits  [MaxAssets]int;
	InQueue [MaxAssets]bool;
	Queue   [MaxAssets + 1]int;
}


func spfa(graph *MarketGraph, state *SPFAState, startNodes []int) ([]int, float64, float64) {

	for i := range graph.AssetCount {
		state.Dist[i] = math.Inf(1);
		state.Parent[i] = -1;
		state.Visits[i] = 0;
		state.InQueue[i] = false;
	}
	head, tail := 0, 0;
	queueLen := len(state.Queue);

	for i := 0; i < graph.AssetCount; i++ {
		state.Dist[i] = math.Inf(1);
		state.Parent[i] = -1;
	}


	for _, node := range startNodes {
		state.Dist[node] = 0;
		state.Queue[tail] = node;
		tail = (tail + 1) % queueLen;
		state.InQueue[node] = true;
	}

	for head != tail {
		u := state.Queue[head];
		head = (head + 1) % queueLen;
		state.InQueue[u] = false;

		for v := 0; v < graph.AssetCount; v++ {
			weight := graph.Weights[u][v];
			if weight == math.Inf(1) {
				continue;
			}

			if state.Dist[u] + weight < state.Dist[v] {
				state.Dist[v] = state.Dist[u] + weight;
				state.Parent[v] = u;

				if !state.InQueue[v] {
					state.Queue[tail] = v
					tail = (tail + 1) % queueLen;
					state.InQueue[v] = true;
					state.Visits[v]++;

					if state.Visits[v] > graph.AssetCount {
						return extractArbitrageCycle(state.Parent, v, graph.AssetCount, graph);
					}
				}
			}
		}
	}

	return nil,0.0,0.0;
}

func extractArbitrageCycle(parent [MaxAssets]int, cycleEnd int, assetCount int, graph *MarketGraph) ([]int, float64, float64) {
	curr := cycleEnd
	for i := 0; i < assetCount; i++ {
		curr = parent[curr]
		if curr == -1 {
			return nil,0.0,0.0; 
		}
	}

	cycleStart := curr
	var reversedCycle []int
	reversedCycle = append(reversedCycle, cycleStart)

	curr = parent[cycleStart]
	for curr != cycleStart {
		reversedCycle = append(reversedCycle, curr)
		curr = parent[curr]
		if curr == -1 {
			return nil,0.0,0.0; 
		}
	}
	reversedCycle = append(reversedCycle, cycleStart) 

	cycleLength := len(reversedCycle)
	forwardCycle := make([]int, cycleLength)
	for i := 0; i < cycleLength; i++ {
		forwardCycle[i] = reversedCycle[cycleLength-1-i]
	}

	compoundRate := 1.0;
	maxStartingCap := math.Inf(1);

	for i := range len(forwardCycle)-1 {
		u := forwardCycle[i];
		v := forwardCycle[i+1];

		rate := graph.Rates[u][v] * (1.0 - tradingFee);
		localCap := graph.Capacities[u][v];

		impliedStartingCap := localCap/compoundRate;
		if impliedStartingCap < maxStartingCap {
			maxStartingCap = impliedStartingCap;
		}

		compoundRate *= rate;
	}

	return forwardCycle, compoundRate, maxStartingCap;

}


func StartFormatterWorker(graph *MarketGraph, rawStream <-chan RawCycleData, hub *EventHub) {

	cooldowns := make(map[string]time.Time);

	for raw := range rawStream {
		var pathBuilder strings.Builder;
		for i, nodeID := range raw.Path {
			if i > 0 {
				pathBuilder.WriteString(" -> ");
			}
			pathBuilder.WriteString(graph.AssetName[nodeID]);
		}
		pathStr := pathBuilder.String();

		if expiry, exists := cooldowns[pathStr]; exists {
			if time.Now().Before(expiry) {
				// The cycle is still on cooldown. Drop it.
				continue ;
			}
		}
		
		// Set a 1-second silence cooldown for this specific path
		cooldowns[pathStr] = time.Now().Add(250 * time.Millisecond);
		atomic.AddUint64(&GlobalCycleCount, 1);

		payload := ArbitragePayload{
			ID:          fmt.Sprintf("%d", time.Now().UnixMilli()),
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
			Path:        pathStr,
			Multiplier:  raw.Multiplier,
			MaxCapacity: raw.Capacity,
		}

		hub.BroadcastArbitrage(payload);

		fmt.Print("\033[18;0H");
		fmt.Printf("\n[PROFIT ROUTE DETECTED] Multiplier: %.5fx | Size: %.2f | Path: %s\n", 
			raw.Multiplier, raw.Capacity, pathStr);
	}
}

func RunArbitrageEngine(graph *MarketGraph, tickStream <-chan TickEvent, rawOut chan<- RawCycleData) {

	engineState := &SPFAState{};

	for tick := range tickStream {
		atomic.AddUint64(&GlobalTickCount, 1);

		if tick.Bid > 0 {
			graph.Weights[tick.RouteID.BaseID][tick.RouteID.QuoteID] = -math.Log(tick.Bid * (1 - tradingFee));
			graph.Rates[tick.RouteID.BaseID][tick.RouteID.QuoteID] = tick.Bid;
			graph.Capacities[tick.RouteID.BaseID][tick.RouteID.QuoteID] = tick.BidQty;
		}

		if tick.Ask > 0 {
			inverseRate := 1.0 / tick.Ask;
			graph.Weights[tick.RouteID.QuoteID][tick.RouteID.BaseID] = -math.Log(inverseRate * (1 - tradingFee));
			graph.Rates[tick.RouteID.QuoteID][tick.RouteID.BaseID] = inverseRate;
			graph.Capacities[tick.RouteID.QuoteID][tick.RouteID.BaseID] = tick.AskQty * tick.Ask;
		}

		cycle,profit,maxStartingCap := spfa(graph, engineState, []int{tick.RouteID.BaseID, tick.RouteID.QuoteID});

		if cycle != nil {

			select {
			case rawOut <- RawCycleData{ Path : cycle , Multiplier: profit , Capacity: maxStartingCap }:
				// Offloading the uploading work to StartFormatterWorker
			default:
				// Channel full. Cycles being dropped.
			}

		}
	}
}
