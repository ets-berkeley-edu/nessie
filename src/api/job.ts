import axios from 'axios';
import store from '@/store';

export function startJob(jobId: number) {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .post(`${apiBaseUrl}/api/job/${jobId}`)
    .then(response => response.data, () => null);
}

export function getBackgroundJobStatus(date: Date) {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  const isoString = date ? date.toISOString() : null;
  return axios
    .post(`${apiBaseUrl}/api/admin/background_job_status?date=${isoString}`)
    .then(response => response.data, () => null);
}

export function getRunnableJobs() {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .get(`${apiBaseUrl}/api/admin/runnable_jobs`)
    .then(response => response.data, () => null);
}

export function runJob(path: string) {
  const apiBaseUrl = store.getters['context/apiBaseUrl'];
  return axios
    .post(`${apiBaseUrl}${path}`)
    .then(response => response.data, () => null);
}
