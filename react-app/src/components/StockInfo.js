import { useEffect, useState } from "react";

const StockInfo = ({ symbol }) => {
  const [stockData, setStockData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
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
  }, [symbol]);

  if (loading) return <p>Loading stock data...</p>;
  if (error) return <p>Error: {error}</p>;

  return (
    <div>
      <h2>{stockData.symbol} Stock Info</h2>
      <p><strong>Date:</strong> {stockData.date}</p>
      <p><strong>Revenue:</strong> ${stockData.revenue?.toLocaleString()}</p>
      <p><strong>EPS:</strong> {stockData.eps}</p>
      <p><strong>P/E Ratio:</strong> {stockData.pe_ratio}</p>
      <p><strong>Market Cap:</strong> ${stockData.market_cap?.toLocaleString()}</p>
      <p><strong>Debt-to-Equity:</strong> {stockData.debt_to_equity}</p>
    </div>
  );
};

export default StockInfo;
