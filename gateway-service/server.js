const express = require('express');
const cors = require('cors');
const axios = require('axios');
const path = require('path');
const winston = require('winston');

// Configure logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});

if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.simple()
  }));
}

const app = express();
const port = 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
  logger.info({
    method: req.method,
    path: req.path,
    query: req.query,
    body: req.body
  });
  next();
});

// Path to your C++ executable
const cppExecutable = path.join(__dirname, '../Database Project/build/Debug/Database.exe');
const dbDataPath = path.join(__dirname, '../Database Project/build/Debug/db_data');

// Log paths
logger.info('C++ Executable Path:', { path: cppExecutable });
logger.info('Database Data Path:', { path: dbDataPath });
logger.info('Current Working Directory:', { cwd: process.cwd() });

// Database state management
let currentDatabase = '';

// Function to communicate with C++ backend
const communicateWithBackend = async (command) => {
  try {
    logger.info(`Executing command: ${command}`);
    
    // If command is USE, update current database
    if (command.startsWith('USE ')) {
      currentDatabase = command.split(' ')[1];
      logger.info(`Current database set to: ${currentDatabase}`);
    }
    
    // Prepare the final command
    let finalCommand = command;
    
    // If we have a current database and the command isn't USE, prepend USE command
    if (currentDatabase && !command.startsWith('USE ')) {
      finalCommand = `USE ${currentDatabase};${command}`;
      logger.info(`Combined command with database context: ${finalCommand}`);
    }
    
    // If creating a table and no database is selected, reject
    if (command.startsWith('CREATE TABLE') && !currentDatabase) {
      throw new Error('No database selected. Use USE database_name first.');
    }
    
    let retries = 3;
    let lastError = null;
    
    while (retries > 0) {
      try {
        const response = await axios.post('http://localhost:8080/query', {
          query: finalCommand
        }, {
          timeout: 5000, // 5 second timeout
          maxRedirects: 0
        });
        
        if (response.data.success) {
          logger.info(`Command executed successfully`, { command: finalCommand });
          break; // Success, exit retry loop
        } else {
          throw new Error(response.data.error || 'Error executing command');
        }
      } catch (error) {
        lastError = error;
        if (error.code === 'ECONNRESET' || error.code === 'ECONNREFUSED') {
          retries--;
          if (retries > 0) {
            logger.warn(`Connection error, retrying... (${retries} attempts left)`);
            await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second before retry
          }
        } else {
          throw error; // Non-connection errors are thrown immediately
        }
      }
    }
    
    if (retries === 0 && lastError) {
      throw lastError;
    }
    
    return 'Command executed successfully';
  } catch (error) {
    logger.error('Command execution failed', { 
      error: error.message,
      command
    });
    throw error;
  }
};

// API endpoints
app.post('/api/query', async (req, res) => {
  try {
    const { query } = req.body;
    if (!query) {
      logger.warn('Query endpoint called without query parameter');
      return res.status(400).json({ success: false, error: 'Query is required' });
    }
    
    const response = await axios.post('http://localhost:8080/query', {
      query: query
    }, {
      timeout: 5000,
      maxRedirects: 0
    });

    // Forward the structured response from the backend
    res.json({
      success: true,
      data: {
        results: response.data.results || [],
        error: response.data.error_message || null,
        recordsFound: response.data.records_found || 0
      }
    });
  } catch (error) {
    logger.error('Query execution failed', { 
      error: error.message,
      query: req.body.query
    });
    res.status(500).json({ 
      success: false, 
      error: error.response?.data?.error_message || error.message 
    });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  const health = {
    status: 'ok',
    timestamp: new Date(),
    uptime: process.uptime(),
    currentDatabase: currentDatabase
  };
  logger.info('Health check performed', health);
  res.json(health);
});

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Unhandled error', { 
    error: err.message,
    stack: err.stack,
    path: req.path
  });
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error'
  });
});

// Start server
app.listen(port, () => {
  logger.info(`Gateway service running on port ${port}`);
}); 