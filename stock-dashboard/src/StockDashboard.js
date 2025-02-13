import { useState, useEffect } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid,
  BarChart, Bar, ResponsiveContainer
} from "recharts";
import "./StockDashboard.css";

function StockDashboard({ ticker }) {
  const [data, setData] = useState([]);
  const [error, setError] = useState(null);
  const [selectedMetrics, setSelectedMetrics] = useState(["Open", "High", "Low", "Close"]); // Default: Show all

  useEffect(() => {
    if (!ticker) return;

    fetch(`http://127.0.0.1:8000/stock/${ticker}`)
      .then((res) => {
        if (!res.ok) throw new Error("Stock not found");
        return res.json();
      })
      .then((data) => {
        setData(data);
        setError(null);
      })
      .catch((error) => setError(error.message));
  }, [ticker]);

  // Handles checkbox selection
  const handleMetricChange = (event) => {
    const { value, checked } = event.target;

    setSelectedMetrics((prevMetrics) => {
      if (checked) {
        return [...prevMetrics, value]; // Add metric if checked
      } else {
        return prevMetrics.length > 1
          ? prevMetrics.filter((metric) => metric !== value) // Remove metric
          : prevMetrics; // Prevent removing all metrics
      }
    });
  };

  return (
    <div className="dashboard-container">
      <h1>{ticker} Stock Data</h1>

      {/* Checkbox Selector */}
      <div className="checkbox-container">
        {["Open", "High", "Low", "Close"].map((metric) => (
          <label key={metric} className="checkbox-label">
            <input
              type="checkbox"
              value={metric}
              checked={selectedMetrics.includes(metric)}
              onChange={handleMetricChange}
            />
            {metric}
          </label>
        ))}
      </div>

      {error ? (
        <p style={{ color: "red" }}>{error}</p>
      ) : (
        <>
          <h2>Stock Prices Over Time</h2>
          <ResponsiveContainer width="90%" height={300}>
            <LineChart data={data}>
              <XAxis dataKey="Datetime" tickFormatter={(tick) => tick.split(" ")[1]} />
              <YAxis domain={["auto", "auto"]} />
              <CartesianGrid strokeDasharray="3 3" />
              <Tooltip />
              <Legend />

              {/* Dynamically render selected metrics */}
              {selectedMetrics.includes("Open") && <Line type="monotone" dataKey="Open" stroke="blue" />}
              {selectedMetrics.includes("High") && <Line type="monotone" dataKey="High" stroke="green" />}
              {selectedMetrics.includes("Low") && <Line type="monotone" dataKey="Low" stroke="red" />}
              {selectedMetrics.includes("Close") && <Line type="monotone" dataKey="Close" stroke="black" />}
            </LineChart>
          </ResponsiveContainer>

          <h2>Volume Over Time</h2>
          <ResponsiveContainer width="90%" height={300}>
            <BarChart data={data}>
              <XAxis dataKey="Datetime" tickFormatter={(tick) => tick.split(" ")[1]} />
              <YAxis width={90} domain={["auto", "auto"]} />
              <CartesianGrid strokeDasharray="3 3" />
              <Tooltip />
              <Bar dataKey="Volume" fill="purple" />
            </BarChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}

export default StockDashboard;
