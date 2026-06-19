package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/valyala/fastjson"
)

func StreamMultiplexer(subscriptions []string, graph *MarketGraph, tickBuffer chan<- TickEvent) {
	if len(subscriptions) == 0 {
		slog.Error("No subscriptions provided to multiplexer");
		os.Exit(1);
	}

	for {
		err := connectAndRead(subscriptions, graph, tickBuffer);

		slog.Error("Network connection severed", "error", err, "action", "initiating_recovery");

		graph.WipeState();
		slog.Info("Matrix memory sanitized");
		
		time.Sleep(3 * time.Second)
	}

}

func connectAndRead(subscriptions []string, graph *MarketGraph, tickBuffer chan<- TickEvent) error {
	seed := subscriptions[0]
	socketURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s", seed)

	
	dialer := &websocket.Dialer{
		ReadBufferSize: 4096,
		WriteBufferSize: 4096,
		HandshakeTimeout: 5 * time.Second,
	}
	
	conn, _, err := dialer.Dial(socketURL, nil);
	if err != nil {
		return fmt.Errorf("dial failed: %w", err);
	}
	defer conn.Close();

	// Prevention against zombie TCP connection
	pongWait := 60 * time.Second;
	conn.SetReadDeadline(time.Now().Add(pongWait));

	// Reseting Deadline every PING frame handling
	conn.SetPingHandler(func (appData string) error {
				conn.SetReadDeadline(time.Now().Add(pongWait));
				return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second));
	});

	chunkSize := 50;
	remainingSubs := subscriptions[1:];

	for i := 0; i < len(remainingSubs); i += chunkSize {
		end := i + chunkSize;
		if end > len(remainingSubs) {
			end = len(remainingSubs);
		}

		subMessage := map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": remainingSubs[i:end],
			"id":     i + 1, // Unique ID per chunk
		}
		
		if err := conn.WriteJSON(subMessage); err != nil {
			return fmt.Errorf("failed to send chunk: %w", err);
		}
		
		time.Sleep(250 * time.Millisecond);
	}

	var parserPool fastjson.ParserPool;

	payloadPool := sync.Pool{
		New: func() any{
			return bytes.NewBuffer(make([]byte, 0, 1024));
		},
	}

	for {
		messageType, r, err := conn.NextReader();
		if err != nil {
			return err;
		}

		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue;
		}

		buf := payloadPool.Get().(*bytes.Buffer);
		buf.Reset();

		_, err = buf.ReadFrom(r);
		if err != nil {
			payloadPool.Put(buf);
			return err;
		}

		p := parserPool.Get()
		v, err := p.ParseBytes(buf.Bytes());
		if err != nil {
			parserPool.Put(p);
			payloadPool.Put(buf);
			continue;
		}

		symBytes := v.GetStringBytes("s");
		if symBytes == nil {
			parserPool.Put(p);
			payloadPool.Put(buf);
			continue;
		}

		route, exists := graph.Routes[string(symBytes)]
		if !exists {
			parserPool.Put(p)
			payloadPool.Put(buf);
			continue
		}

		bidBytes := v.GetStringBytes("b")
		askBytes := v.GetStringBytes("a")
		bidQtyBytes := v.GetStringBytes("B")
		askQtyBytes := v.GetStringBytes("A")

		if len(bidBytes) > 0 && len(askBytes) > 0 {
			bid, _ := strconv.ParseFloat(string(bidBytes), 64)
			ask, _ := strconv.ParseFloat(string(askBytes), 64)
			askQty, _ := strconv.ParseFloat(string(askQtyBytes), 64);
			bidQty, _ := strconv.ParseFloat(string(bidQtyBytes), 64);

			tick := TickEvent{
				RouteID: route,
				Bid:     bid,
				Ask:     ask,
				BidQty:  bidQty,
				AskQty:  askQty,
			}
			select {
				case tickBuffer <- tick:
				default:
					slog.Warn("Tick buffer full. Dropping market data.");
			}
		}
		
		parserPool.Put(p)
		payloadPool.Put(buf);
	}
}


func fetchLiveExchangeInfo() (*ExchangeInfo, error) {
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://api.binance.com/api/v3/exchangeInfo")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var info ExchangeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, err
}
