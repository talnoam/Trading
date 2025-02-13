import { useState, useEffect } from "react";
import StockDashboard from "./StockDashboard";

function App() {
  const [ticker, setTicker] = useState("AAPL"); // Stores user input
  const [debouncedTicker, setDebouncedTicker] = useState("AAPL"); // The actual ticker used in API calls

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedTicker(ticker); // Update API call only after typing stops
    }, 1000); // 500ms delay (adjust as needed)

    return () => clearTimeout(handler); // Cleanup timeout if user keeps typing
  }, [ticker]);

  return (
    <div>
      <h1>Stock Data Dashboard</h1>

      {/* Input field */}
      <input
        type="text"
        value={ticker}
        onChange={(e) => setTicker(e.target.value.toUpperCase())}
        placeholder="Enter stock ticker (e.g., TSLA, GOOG)"
      />

      {/* The StockDashboard only updates when typing stops */}
      <StockDashboard ticker={debouncedTicker} />
    </div>
  );
}

export default App;
