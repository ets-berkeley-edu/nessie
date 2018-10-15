import axios from 'axios';
import store from '@/store';

export function startJob(jobId: number) {
  return axios
    .post(`${store.state.apiBaseURL}/api/job/${jobId}`)
    .then(response => response.data);
}
