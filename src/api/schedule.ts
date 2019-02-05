import axios from 'axios';
import store from '@/store';

export function getJobSchedule(jobId: string) {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .post(`${apiBaseUrl}/api/schedule/${jobId}`)
    .then(response => response.data, err => err.response);
}

export function getSchedule() {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .get(`${apiBaseUrl}/api/schedule`)
    .then(response => response.data, err => err.response);
}
