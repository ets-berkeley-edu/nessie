import axios from 'axios';
import store from '@/store';

export function startJob(jobId: number) {
  return axios
    .post(`${store.state.apiBaseURL}/api/job/${jobId}`)
    .then(response => response.data, () => null);
}

export function getBackgroundJobStatus(date: Date) {
  const isoString = date ? date.toISOString() : null;
  return axios
    .post(
      `${
        store.state.apiBaseURL
      }/api/admin/background_job_status?date=${isoString}`
    )
    .then(response => response.data, () => null);
}

export function getRunnableJobs() {
  return axios
    .get(`${store.state.apiBaseURL}/api/admin/runnable_jobs`)
    .then(response => response.data, () => null);
}

export function runJob(path: string) {
  return axios
    .post(`${store.state.apiBaseURL}${path}`)
    .then(response => response.data, () => null);
}
