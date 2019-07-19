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

export function reloadSchedule() {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .post(`${apiBaseUrl}/api/schedule/reload`)
    .then(response => response.data, err => err.response);
}

export function removeSchedule(jobId: string) {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .delete(`${apiBaseUrl}/api/schedule/${jobId}`)
    .then(response => response.data, err => err.response);
}

export function updateSchedule(jobId: string, schedule: object) {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .post(`${apiBaseUrl}/api/schedule/${jobId}`, schedule)
    .then(response => response.data, err => err.response);
}
