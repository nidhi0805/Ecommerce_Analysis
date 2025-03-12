import { Container, Typography } from "@mui/material";
import StockInfo from "../components/StockInfo";

const StockInfoPage = () => {
  return (
    <Container>
      <Typography variant="h4" sx={{ textAlign: "center", my: 3, fontWeight: "bold", color: "#1E88E5" }}>
        Stock Information Dashboard
      </Typography>
      <StockInfo />
    </Container>
  );
};

export default StockInfoPage;
