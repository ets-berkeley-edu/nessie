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
    .then(response => response.data);
}

export function getFailuresFromLastSync() {
  return axios
    .post(`${store.state.apiBaseURL}/api/metadata/failures_from_last_sync`)
    .then(response => response.data, err => err.response);
}
