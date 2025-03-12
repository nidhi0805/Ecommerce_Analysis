import { useEffect, useState } from "react";
import { Card, CardContent, Typography, CircularProgress, Alert, Grid, MenuItem, Select, FormControl, InputLabel, Paper, Box } from "@mui/material";

const StockInfo = () => {
  const [tickers, setTickers] = useState([]);
  const [selectedTicker, setSelectedTicker] = useState("");
  const [stockData, setStockData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hideLabel, setHideLabel] = useState(false); // Controls visibility of input label

  // Fetch all stock tickers
  useEffect(() => {
    fetch("http://127.0.0.1:8000/tickers")
      .then(response => response.json())
      .then(data => setTickers(data.tickers))
      .catch(err => console.error("Error fetching tickers:", err));
  }, []);

  // Fetch stock data for selected ticker
  const fetchStockData = (symbol) => {
    setLoading(true);
    setError(null);
    setStockData(null);
    setHideLabel(true); // Hide input label after selection

    fetch(`http://127.0.0.1:8000/stock/${symbol}`)
      .then(response => response.json())
      .then(data => {
        if (data.error) throw new Error(data.error);
        setStockData(data);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  };

  return (
    <Grid container spacing={3} sx={{ height: "100vh", padding: 2, justifyContent: "space-between" }}>
      {/* Center - Stock Information */}
      <Grid item xs={9}>
        {loading && <CircularProgress sx={{ display: "block", margin: "auto" }} />}
        {error && <Alert severity="error">{error}</Alert>}

        {stockData && (
          <Card sx={{ boxShadow: 4, borderRadius: 2, padding: 2, backgroundColor: "#f9f9f9" }}>
            <CardContent>
              <Typography variant="h5" sx={{ fontWeight: "bold", color: "#1565C0" }}>
                {stockData.symbol} Stock Info
              </Typography>
              <Typography color="text.secondary" sx={{ mb: 2 }}>
                Date: {stockData.date}
              </Typography>

              <Grid container spacing={2}>
                <Grid item xs={6}>
                  <Typography><strong>Revenue:</strong> ${stockData.revenue?.toLocaleString()}</Typography>
                  <Typography><strong>EPS:</strong> {stockData.eps}</Typography>
                </Grid>
                <Grid item xs={6}>
                  <Typography><strong>P/E Ratio:</strong> {stockData.pe_ratio.toFixed(2)}</Typography>
                  <Typography><strong>Market Cap:</strong> ${stockData.market_cap?.toLocaleString()}</Typography>
                  <Typography><strong>Debt-to-Equity:</strong> {stockData.debt_to_equity}</Typography>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        )}
      </Grid>

      {/* Right Sidebar - Stock Selector */}
      <Grid item xs={3} sx={{ display: "flex", justifyContent: "flex-end" }}>
        <Paper elevation={3} sx={{ padding: 3, borderRadius: 2, backgroundColor: "#f5f5f5" }}>
          <Typography variant="h6" gutterBottom sx={{ fontWeight: "bold", color: "#1565C0" }}>
            Select a Stock
          </Typography>
          <FormControl fullWidth>
            {!hideLabel && <InputLabel sx={{ display: selectedTicker ? "none" : "block" }}>Choose a Stock</InputLabel>} {/* Hides after selection */}
            <Select
              value={selectedTicker}
              onChange={(e) => { setSelectedTicker(e.target.value); fetchStockData(e.target.value); }}
              sx={{ backgroundColor: "white", borderRadius: 1 }}
            >
              {tickers.map((ticker) => (
                <MenuItem key={ticker} value={ticker}>{ticker}</MenuItem>
              ))}
            </Select>
          </FormControl>
        </Paper>
      </Grid>

      {/* Bottom Section - Financial Term Explanations */}
      <Grid item xs={12}>
        <Paper elevation={3} sx={{ padding: 3, marginTop: 3, backgroundColor: "#ffffff" }}>
          <Typography variant="h6" sx={{ fontWeight: "bold", color: "#1565C0" }}>
            What Do These Terms Mean?
          </Typography>
          <Typography><strong>Revenue:</strong> Total earnings from sales before expenses.</Typography>
          <Typography><strong>EPS (Earnings Per Share):</strong> Profit divided by total shares outstanding.</Typography>
          <Typography><strong>P/E Ratio:</strong> Price-to-Earnings ratio. High means investors expect growth.</Typography>
          <Typography><strong>Market Cap:</strong> Total company value (Stock Price × Shares).</Typography>
          <Typography><strong>Debt-to-Equity:</strong> Measures financial leverage (higher means more debt).</Typography>
        </Paper>
      </Grid>
    </Grid>
  );
};

export default StockInfo;
