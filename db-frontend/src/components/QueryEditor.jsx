// src/components/QueryEditor.jsx
import React from 'react';
import Editor from '@monaco-editor/react';
import { FiPlay, FiSave } from 'react-icons/fi';
import './QueryEditor.css';

const QueryEditor = ({ query, onChange, onRun, loading }) => {
  const handleEditorChange = (value) => {
    onChange(value);
  };

  return (
    <div className="query-editor">
      <div className="toolbar">
        <button onClick={onRun} disabled={loading}>
          <FiPlay /> {loading ? 'Executing...' : 'Run'}
        </button>
        
      </div>
      <Editor
        height="100%"
        defaultLanguage="sql"
        value={query}
        onChange={handleEditorChange}
        options={{
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          fontSize: 14,
          wordWrap: 'on',
          automaticLayout: true,
        }}
      />
    </div>
  );
};

export default QueryEditor;