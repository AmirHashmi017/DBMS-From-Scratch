// src/App.js
import React, { useState, useEffect } from 'react';
import SplitPane from 'react-split-pane';
import Sidebar from './components/Sidebar';
import QueryEditor from './components/QueryEditor';
import ResultsView from './components/ResultsView';
import { fetchDatabaseStructure, executeQuery } from './services/api';
import './App.css';

function App() {
  const [query, setQuery] = useState('SELECT * FROM users;');
  const [results, setResults] = useState(null);
  const [structure, setStructure] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [executionTime, setExecutionTime] = useState(0);
  const [recordsFound, setRecordsFound] = useState(0);

  useEffect(() => {
    // Load database structure on initial render
    
  }, []);

  const handleRunQuery = async () => {
    if (!query.trim()) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const startTime = performance.now();
      const response = await executeQuery(query);
      const endTime = performance.now();
      
      if (response.data.success) {
        setResults(response.data.data.results);
        setRecordsFound(response.data.data.recordsFound);
        setError(response.data.data.error);
      } else {
        setError(response.data.error || 'Query execution failed');
        setResults(null);
      }
      setExecutionTime((endTime - startTime).toFixed(2));
    } catch (err) {
      setError(err.userMessage || 'Query execution failed');
      setResults(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <SplitPane split="vertical" defaultSize={250} minSize={200} maxSize={400}>
        <Sidebar structure={structure} onTableSelect={(table) => setQuery(`SELECT * FROM ${table} LIMIT 100;`)} />
        <SplitPane split="horizontal" defaultSize="40%" minSize={100}>
          <QueryEditor 
            query={query} 
            onChange={setQuery} 
            onRun={handleRunQuery} 
            loading={loading} 
          />
          <ResultsView 
            results={results} 
            error={error} 
            loading={loading} 
            executionTime={executionTime}
            recordsFound={recordsFound}
          />
        </SplitPane>
      </SplitPane>
    </div>
  );
}

export default App;