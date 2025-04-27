// src/components/Sidebar.jsx
import React from 'react';
import { FiDatabase, FiTable, FiColumns, FiKey } from 'react-icons/fi';
import './Sidebar.css';

const Sidebar = ({ structure, onTableSelect }) => {
  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h3>🔥 BlazeDB Explorer</h3>
      </div>
      <div className="sidebar-content">
        {Object.keys(structure).map((dbName) => (
          <div key={dbName} className="database">
            <div className="database-header">
              <FiDatabase className="icon" />
              <span>{dbName}</span>
            </div>
            {structure[dbName].tables && Object.keys(structure[dbName].tables).map((tableName) => (
              <div key={tableName} className="table" onClick={() => onTableSelect(tableName)}>
                <div className="table-header">
                  <FiTable className="icon" />
                  <span>{tableName}</span>
                </div>
                <div className="columns">
                  {structure[dbName].tables[tableName].columns.map((column) => (
                    <div key={column.name} className="column">
                      <FiColumns className="icon" />
                      <span>{column.name}</span>
                      <span className="type">{column.type}</span>
                      {column.primary && <FiKey className="primary-key" title="Primary Key" />}
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
};

export default Sidebar;