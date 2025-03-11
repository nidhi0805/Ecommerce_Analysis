import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import StockInfoPage from "./pages/StockInfoPage";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<StockInfoPage />} />
        <Route path="/stock-info" element={<StockInfoPage />} />
      </Routes>
    </Router>
  );
}

export default App;
