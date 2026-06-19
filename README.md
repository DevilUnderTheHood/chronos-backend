```
# Chronos: High-Frequency Arbitrage Engine

<p align="center">
  <b>A zero-allocation, lock-free High-Frequency Trading (HFT) backend designed to detect microsecond triangular arbitrage cycles in cryptocurrency order books.</b>
</p>

---

## ⚡ Overview

Chronos is a proof-of-concept quantitative trading engine. It treats live cryptocurrency order books as a directed graph and utilizes the **Shortest Path Faster Algorithm (SPFA)** to detect negative-weight cycles (arbitrage opportunities). 

To compete at sub-millisecond latencies on consumer-grade hardware, Chronos completely bypasses the Go Garbage Collector (GC), uses stack-allocated matrices, and routes data through lock-free concurrency channels.

### Key Innovations
* **Zero-Allocation Ingestion:** Utilizes `valyala/fastjson` and `sync.Pool` for byte-to-float conversions, processing thousands of WebSocket payloads per second without touching the heap.
* **Lock-Free Actor Model:** Network ingestion, mathematical scanning, WebSocket broadcasting, and database logging are completely decoupled via Go channels. The math thread is *never* blocked by I/O.
* **Dynamic Topology Mesh:** On boot, the engine calculates the "Degree Centrality" of the Binance exchange to filter out illiquid pairs and generate a dense, L1-cache-optimized mesh of the top 100 highly interconnected assets.
* **Asynchronous Cold Storage:** Leverages Supabase purely as an asynchronous ledger. Cycles are batched and written in the background, keeping the core pipeline strictly forward-looking.

---

## 🏗️ Architecture & Data Flow

1. **Ingester (`ingester.go`)**: Multiplexes Binance WebSockets, decodes JSON payloads without allocation, and pumps `TickEvent`s into a ring buffer.
2. **Engine (`engine.go`)**: Updates a mathematically contiguous 2D float matrix with logarithmic weights `(-ln(Rate * (1 - Fee)))`. Pulses the SPFA algorithm from the nodes affected by the price shock.
3. **Event Hub (`hub.go`)**: A lock-free Pub/Sub broadcaster that distributes detected cycles to downstream workers.
4. **Gateway (`server.go`)**: Upgrades HTTP connections to WebSockets to stream live TPS metrics and execution paths to decoupled frontend UI dashboards.
5. **Database Worker (`supabase_worker.go`)**: Accumulates arbitrage payloads and executes bulk `INSERT`s to Supabase every 5 seconds.

---

## 🚀 Installation & Setup

### Prerequisites
* **Go 1.22+** (For modern standard library features)
* **Supabase Account** (For the asynchronous historical ledger)

### 1. Supabase Database Schema
Create a table named `arbitrage_ledger` in your Supabase project with the following columns:
* `id` (text, primary key)
* `timestamp` (timestampz)
* `path` (text)
* `multiplier` (float8)
* `maxCapacity` (float8)

### 2. Environment Variables
Create a `.env` file or export the following variables in your terminal:
```bash
export SUPABASE_URL="[https://your-project-id.supabase.co](https://your-project-id.supabase.co)"
export SUPABASE_KEY="your-anon-or-service-role-key"

```

### 3. Hardware-Optimized Compilation

Do **not** use standard `go run .` or standard `go build`. To achieve microsecond latency, you must compile with hardware-specific optimizations.

Run the following command to compile the binary. `GOAMD64=v3` forces the compiler to utilize **AVX2 vector instructions** for parallel floating-point mathematics:

```bash
env GOAMD64=v3 go build -ldflags="-s -w" -o spfa_engine .

```

### 4. Execution

To run the engine without Garbage Collection latency spikes, execute the binary with the following runtime flags:

```bash
GOMAXPROCS=6 GOGC=1000 ./spfa_engine

```

* `GOMAXPROCS=6`: Pins the Goroutines to match hardware threads, maintaining L1 cache locality.
* `GOGC=1000`: Delays the Garbage Collector until the heap grows by 1000%, effectively eliminating GC pauses during active market ingestion.

---

## 🔌 WebSocket API Reference (For Frontend)

The backend exposes a single Fire-and-Forget WebSocket endpoint for frontend dashboards to consume live market data.

**Endpoint:** `ws://<backend-ip>:8080/ws`

### Payload 1: Live Metrics (Emitted every 500ms)

Updates the frontend on the engine's health and throughput.

```json
{
  "type": "METRICS_UPDATE",
  "data": {
    "tps": 2845,
    "uptimeSeconds": 142.5,
    "totalCycleCount": 102
  }
}

```

### Payload 2: Arbitrage Detected (Emitted dynamically)

Fires the exact moment the SPFA algorithm traces a negative-weight cycle.

```json
{
  "type": "ARBITRAGE_FOUND",
  "data": {
    "id": "1718645149123",
    "timestamp": "2026-06-19T18:05:49.123Z",
    "path": "USDT -> ETH -> BTC -> USDT",
    "multiplier": 1.0024,
    "maxCapacity": 542.12
  }
}

```

---

## 🗺️ Project Roadmap

* [x] **Phase 1:** Zero-Allocation SPFA Engine & Lock-Free Channels.
* [x] **Phase 2:** Live Market Multiplexing & Topological Mesh Mapping.
* [x] **Phase 3:** Order Book Depth analysis to calculate `maxCapacity` strictly based on available volume, eliminating Ghost Liquidity.
* [x] **Phase 4:** Asynchronous Supabase cold-storage logging.
* [ ] **Phase 5:** Integration of a Charmbracelet `wish/bubbletea` SSH terminal interface for secure, remote TUI management.
* [ ] **Phase 6:** FIX/REST API execution pipelines for live Immediate-Or-Cancel (IOC) limit orders.

---

## ⚠️ Disclaimer

This software is provided for educational and hackathon demonstration purposes only. High-frequency algorithmic trading involves substantial risk of loss. The authors are not responsible for financial losses incurred by using or modifying this codebase.

```

```
