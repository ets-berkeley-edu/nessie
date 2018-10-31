import axios from 'axios';
import store from '@/store';

export function startJob(jobId: number) {
  return axios
    .post(`${store.state.apiBaseURL}/api/job/${jobId}`)
    .then(response => response.data, err => err.response);
}

export function getBackgroundJobStatus(date: Date) {
  const isoString = date ? date.toISOString() : null;
  return axios
    .post(
      `${
        store.state.apiBaseURL
      }/api/metadata/background_job_status?date=${isoString}`
    )
    .then(response => response.data, err => err.response);
}

export function getRunnableJobs() {
  return axios
    .get(`${store.state.apiBaseURL}/api/admin/runnable_jobs`)
    .then(response => response.data, err => err.response);
}
