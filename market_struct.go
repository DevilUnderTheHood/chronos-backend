package engine

type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

type SymbolInfo struct {
	Symbol     string `json:"symbol"`
	Status     string `json:"status"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
}

type EdgeRoute struct {
	BaseID  int;
	QuoteID int;
}

type TickEvent struct {
	RouteID EdgeRoute;
	Bid     float64;
	Ask     float64;
	BidQty  float64;
	AskQty	float64;
}

type MarketGraph struct {
	Weights [MaxAssets][MaxAssets]float64; // Fixed adjacency matrix for log weights
	Rates      [MaxAssets][MaxAssets]float64 // The raw multiplier
	Capacities [MaxAssets][MaxAssets]float64 // Stored in terms of the SOURCE node

	AssetIDs map[string]int; // Map to lookup the integer val associated with the ticker strings.
	AssetName [MaxAssets]string; //Reverse lookup array
	Routes map[string]EdgeRoute;
	AssetCount int;
}
