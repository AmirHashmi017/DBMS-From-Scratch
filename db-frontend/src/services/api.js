// src/services/api.js
import axios from 'axios';
import axiosRetry from 'axios-retry';

const API_BASE_URL = 'http://localhost:3001/api'; // Gateway service URL

// Create axios instance with custom config
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000
});

// Configure retry behavior
axiosRetry(apiClient, { 
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  retryCondition: (error) => {
    // Retry on network errors or 5xx server errors
    return axiosRetry.isNetworkOrIdempotentRequestError(error) || 
           (error.response && error.response.status >= 500);
  }
});

// Request interceptor for logging
apiClient.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method.toUpperCase()} ${config.url}`, config.data);
    return config;
  },
  (error) => {
    console.error('API Request Error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => {
    console.log('API Response:', response.data);
    return response;
  },
  (error) => {
    let errorMessage = 'An unexpected error occurred';
    
    if (error.response) {
      // Server responded with error
      errorMessage = error.response.data?.error || `Server error: ${error.response.status}`;
      console.error('API Error Response:', {
        status: error.response.status,
        data: error.response.data
      });
    } else if (error.request) {
      // Request made but no response
      errorMessage = 'No response from server';
      console.error('API No Response:', error.request);
    } else {
      // Request setup error
      errorMessage = error.message;
      console.error('API Setup Error:', error.message);
    }

    return Promise.reject({
      ...error,
      userMessage: errorMessage
    });
  }
);

export const fetchDatabaseStructure = async () => {
  try {
    const response = await apiClient.get('/structure');
    return response.data;
  } catch (error) {
    console.error('Failed to fetch database structure:', error);
    throw error;
  }
};

export const executeQuery = async (query) => {
  try {
    const response = await apiClient.post('/query', { query });
    return response;
  } catch (error) {
    console.error('Failed to execute query:', error);
    throw error;
  }
};