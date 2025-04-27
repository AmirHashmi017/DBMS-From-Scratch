// src/services/api.js
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8080'; // Change this to your C++ backend URL

export const fetchDatabaseStructure = async () => {
  try {
    const response = await axios.get(`${API_BASE_URL}/structure`);
    return response.data;
  } catch (error) {
    throw error;
  }
};

export const executeQuery = async (query) => {
  try {
    const response = await axios.post(`${API_BASE_URL}/query`, { query });
    return response;
  } catch (error) {
    throw error;
  }
};