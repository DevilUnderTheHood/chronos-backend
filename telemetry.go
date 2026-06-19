package main

import (
	"fmt"
	"sync/atomic"
	"time"
)


func StartTelemetry(graph *MarketGraph) {
	fmt.Print("\033[2J");

	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for range ticker.C {
			ticks := atomic.LoadUint64(&GlobalTickCount)
			cycles := atomic.LoadUint64(&GlobalCycleCount)
			elapsed := time.Since(GlobalStartTime).Seconds()
			tps := float64(ticks) / elapsed

			fmt.Print("\033[H")

			fmt.Printf("===================================================================\n")
			fmt.Printf("                   SPFA ARBITRAGE ENGINE [LIVE]                    \n")
			fmt.Printf("===================================================================\n")
			fmt.Printf(" Topology: %d Assets | %d Routes | Uptime: %8.2fs \n", graph.AssetCount, len(graph.Routes), elapsed)
			fmt.Printf(" Ticks   : %-10d (%.0f tps)    | Cycles Found: %d \n", ticks, tps, cycles)
			fmt.Printf("===================================================================\n")
			// fmt.Printf(" CORE MATRIX LOG-WEIGHTS (Top 10 Assets)                            \n")
			// fmt.Printf("-------------------------------------------------------------------\n")
			//
			// gridSize := 15
			// if gridSize > graph.AssetCount {
			// 	gridSize = graph.AssetCount
			// }
			//
			// // Dynamically calculate the terminal width required
			// // 6 chars for the left column label + (8 chars per asset column)
			// lineWidth := 6 + (8 * gridSize)
			// divider := strings.Repeat("-", lineWidth)
			// thickDivider := strings.Repeat("=", lineWidth)
			//
			// // --- MATRIX HEADER ROW ---
			// fmt.Printf("     |")
			// for i := 0; i < gridSize; i++ {
			// 	// 1 space + 5 char string + 1 space + 1 pipe = 8 chars
			// 	fmt.Printf(" %-5s |", graph.AssetName[i])
			// }
			// fmt.Printf("\n%s\n", divider)
			//
			// // --- MATRIX DATA ROWS ---
			// for i := 0; i < gridSize; i++ {
			// 	fmt.Printf("%-4s |", graph.AssetName[i]) // Row Label
			//
			// 	for j := 0; j < gridSize; j++ {
			// 		weight := graph.Weights[i][j]
			//
			// 		if math.IsInf(weight, 1) {
			// 			fmt.Printf("\033[90m  +Inf  \033[0m|")
			// 		} else if i == j {
			// 			fmt.Printf("\033[90m 0.0000 \033[0m|")
			// 		} else {
			// 			// %7.4f forces the float to take 7 characters, plus 1 space at the end
			// 			fmt.Printf("\033[96m%7.4f \033[0m|", weight)
			// 		}
			// 	}
			// 	fmt.Printf("\n")
			// }
			// fmt.Printf("%s\n", thickDivider)
		}
	}()
}
