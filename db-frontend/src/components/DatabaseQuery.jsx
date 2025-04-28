import React, { useState } from 'react';
import axios from 'axios';

const DatabaseQuery = () => {
    const [query, setQuery] = useState('');
    const [result, setResult] = useState('');
    const [databases, setDatabases] = useState([]);
    const [tables, setTables] = useState([]);
    const [selectedDatabase, setSelectedDatabase] = useState('');

    const API_BASE_URL = 'http://localhost:8080';

    const executeQuery = async () => {
        try {
            const response = await axios.post(`${API_BASE_URL}/query`, {
                query: query
            });
            setResult(JSON.stringify(response.data, null, 2));
        } catch (error) {
            setResult(`Error: ${error.response?.data?.message || error.message}`);
        }
    };

    const fetchDatabases = async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/databases`);
            setDatabases(response.data.databases);
        } catch (error) {
            console.error('Error fetching databases:', error);
        }
    };

    const fetchTables = async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/tables`);
            setTables(response.data.tables);
        } catch (error) {
            console.error('Error fetching tables:', error);
        }
    };

    const useDatabase = async (dbName) => {
        try {
            await axios.post(`${API_BASE_URL}/use-database`, {
                database: dbName
            });
            setSelectedDatabase(dbName);
            fetchTables();
        } catch (error) {
            console.error('Error switching database:', error);
        }
    };

    return (
        <div className="database-query">
            <h2>Database Query Interface</h2>
            
            <div className="database-selection">
                <h3>Databases</h3>
                <button onClick={fetchDatabases}>Refresh Databases</button>
                <ul>
                    {databases.map(db => (
                        <li key={db}>
                            <button 
                                onClick={() => useDatabase(db)}
                                className={selectedDatabase === db ? 'selected' : ''}
                            >
                                {db}
                            </button>
                        </li>
                    ))}
                </ul>
            </div>

            <div className="tables-list">
                <h3>Tables in {selectedDatabase || 'No Database Selected'}</h3>
                <button onClick={fetchTables}>Refresh Tables</button>
                <ul>
                    {tables.map(table => (
                        <li key={table}>{table}</li>
                    ))}
                </ul>
            </div>

            <div className="query-input">
                <h3>Enter Query</h3>
                <textarea
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Enter your SQL query here..."
                    rows="5"
                />
                <button onClick={executeQuery}>Execute Query</button>
            </div>

            <div className="query-result">
                <h3>Result</h3>
                <pre>{result}</pre>
            </div>
        </div>
    );
};

export default DatabaseQuery; 