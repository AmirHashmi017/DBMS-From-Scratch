// src/components/ResultsView.jsx
import React from 'react';
import ReactJson from 'react-json-view';
import './ResultsView.css';

const ResultsView = ({ results, error, loading, executionTime, recordsFound }) => {
  return (
    <div className="results-view">
      <div className="results-header">
        {loading ? (
          <div className="status">Executing query...</div>
        ) : error ? (
          <div className="status error">{error}</div>
        ) : results ? (
          <div className="status success">
            Query executed in {executionTime} ms. {recordsFound} records found.
          </div>
        ) : (
          <div className="status">Results will appear here</div>
        )}
      </div>
      <div className="results-content">
        {loading && <div className="loading-spinner"></div>}
        {error && !loading && (
          <div className="error-message">
            <pre>{error}</pre>
          </div>
        )}
        {results && !loading && (
          <div className="results-table">
            {Array.isArray(results) ? (
              <table>
                <thead>
                  <tr>
                    {results.length > 0 && Object.keys(results[0]).map((key) => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {results.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((value, j) => (
                        <td key={j}>{typeof value === 'object' ? JSON.stringify(value) : String(value)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <ReactJson src={results} theme="monokai" collapsed={1} />
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ResultsView;