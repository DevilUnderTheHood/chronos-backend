package main

import (
	"sync"
	"sync/atomic"
)

type MetricsPayload struct {
	TPS           uint64  `json:"tps"`
	UptimeSeconds float64 `json:"uptimeSeconds"`
	TotalCycleCount uint64 `json:"totalCycleCount"`
}

type ArbitragePayload struct {
	ID          string  `json:"id"`
	Timestamp   string  `json:"timestamp"`
	Path        string  `json:"path"`
	Multiplier  float64 `json:"multiplier"`
	MaxCapacity float64 `json:"maxCapacity"`
	INRProfit		float64 `json:"inrProfit"`
}

type EventHub struct {
	mu      sync.Mutex;
	arbSubs atomic.Value; 
}

func NewEventHub() *EventHub {
	h := &EventHub{};
	h.arbSubs.Store(make([]chan ArbitragePayload, 0));
	return h;
}

// Returns a dedicated channel to the subscriber
func (h *EventHub) SubscribeArbitrage() chan ArbitragePayload {
	h.mu.Lock();
	defer h.mu.Unlock();

	ch := make(chan ArbitragePayload, 1000);
	oldSubs := h.arbSubs.Load().([]chan ArbitragePayload);
	newSubs := make([]chan ArbitragePayload, len(oldSubs)+1);
	copy(newSubs, oldSubs);
	newSubs[len(oldSubs)] = ch;
	h.arbSubs.Store(newSubs);

	return ch;
}

// Lock free arbitrage brodcaster
func (h *EventHub) BroadcastArbitrage(payload ArbitragePayload) {
	subs := h.arbSubs.Load().([]chan ArbitragePayload);
	
	for _, ch := range subs {
		select {
		case ch <- payload:
		default:
			// Subscriber full right now. Drop the packets.
		}
	}
}
