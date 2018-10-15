import axios from 'axios';
import store from '@/store';

export function getJobSchedule(jobId: number) {
  return axios
    .post(`${store.state.apiBaseURL}/api/schedule/${jobId}`)
    .then(response => response.data);
}

export function getSchedule() {
  return axios
    .get(`${store.state.apiBaseURL}/api/schedule`)
    .then(response => response.data);
}
