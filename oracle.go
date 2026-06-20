package main

import (
	"fmt"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

type PriceCache map[string]float64;

type PricingOracle struct {
	cache atomic.Value;
}

func NewPricingOracle(baseAssets []string) *PricingOracle {
	oracle := &PricingOracle{};
	
	oracle.cache.Store(make(PriceCache));

	go oracle.startForexWorker();
	go oracle.startCryptoWorker(baseAssets);

	return oracle;
}

func (o *PricingOracle) GetPrice(asset string) float64 {
	cache := o.cache.Load().(PriceCache);
	if val, exists := cache[asset]; exists {
		return val;
	}
	// Fallback for stablecoins
	if asset == "USDT" || asset == "USDC" || asset == "FDUSD" {
		return 1.0;
	}
	return 0.0;
}

func (o *PricingOracle) startForexWorker() {
	ticker := time.NewTicker(6 * time.Hour);
	defer ticker.Stop();

	updateINR := func() {
		resp, err := http.Get("https://open.er-api.com/v6/latest/USD");
		if err != nil {
			slog.Warn("Oracle failed to fetch INR rate", "error", err);
			return
		}
		defer resp.Body.Close();

		var data struct {
			Rates struct {
				INR float64 `json:"INR"`
			} `json:"rates"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&data); err == nil && data.Rates.INR > 0 {
			oldCache := o.cache.Load().(PriceCache);
			newCache := make(PriceCache);
			for k, v := range oldCache { newCache[k] = v };
			
			newCache["INR"] = data.Rates.INR;
			o.cache.Store(newCache);
			slog.Info("Oracle updated Forex rate", "USD_INR", data.Rates.INR);
		}
	}

	updateINR();
	for range ticker.C {
		updateINR();
	}
}

func (o *PricingOracle) startCryptoWorker(assets []string) {
	ticker := time.NewTicker(3 * time.Second);
	defer ticker.Stop();

	client := &http.Client{Timeout: 2 * time.Second};

	for range ticker.C {
		volatileAssets := []string{};
		for _, a := range assets {
			if a != "USDT" && a != "USDC" && a != "FDUSD" {
				volatileAssets = append(volatileAssets, a);
			}
		}

		if len(volatileAssets) == 0 {
			continue;
		}

		resp, err := client.Get("https://api.binance.com/api/v3/ticker/price");
		if err != nil {
			continue;
		}
		
		var data []struct {
			Symbol string `json:"symbol"`
			Price  string `json:"price"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&data); err == nil {
			oldCache := o.cache.Load().(PriceCache);
			newCache := make(PriceCache);
			for k, v := range oldCache { newCache[k] = v };

			directFound := make(map[string]bool);

			for _, item := range data {
				for _, asset := range volatileAssets {
					if item.Symbol == asset+"USDT" {
						var price float64;
						fmt.Sscanf(item.Price, "%f", &price);
						newCache[asset] = price;
						directFound[asset] = true;
					}

					if item.Symbol == "USDT"+asset {
						var invertedPrice float64
						fmt.Sscanf(item.Price, "%f", &invertedPrice)
						
						if invertedPrice > 0 && !directFound[asset] {
							newCache[asset] = 1.0 / invertedPrice
						}
					}
				}
			}
			o.cache.Store(newCache);
		}
		resp.Body.Close();
	}
}
