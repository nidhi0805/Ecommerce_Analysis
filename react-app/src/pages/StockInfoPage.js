import StockInfo from "../components/StockInfo";

const StockInfoPage = () => {
  return (
    <div>
      <h1>Stock Information</h1>
      <StockInfo symbol="MMM" /> {/* Example for Apple */}
    </div>
  );
};

export default StockInfoPage;
