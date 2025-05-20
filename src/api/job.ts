import axios from 'axios'
import {useContextStore} from '@/stores/context'

export function startJob(jobId: number) {
  return axios.post(`${useContextStore().config.apiBaseUrl}/api/job/${jobId}`)
}

export function getBackgroundJobStatus(date: Date) {
  let url = `${useContextStore().config.apiBaseUrl}/api/admin/background_job_status`
  if (date) {
    url = url.concat(`?date=${date.toISOString()}`)
  }
  return axios.post(url)
}

export function getRunnableJobs() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/admin/runnable_jobs`)
}

export function runJob(path: string) {
  return axios.post(`${useContextStore().config.apiBaseUrl}${path}`)
}
