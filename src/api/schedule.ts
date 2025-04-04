import axios from 'axios'
import {useContextStore} from '@/stores/context'

export function getSchedule() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/schedule`)
}

export function reloadSchedule() {
  return axios.post(`${useContextStore().config.apiBaseUrl}/api/schedule/reload`)
}

export function removeSchedule(jobId: string) {
  return axios.delete(`${useContextStore().config.apiBaseUrl}/api/schedule/${jobId}`)
}

export function updateSchedule(jobId: string, schedule: object) {
  return axios.post(`${useContextStore().config.apiBaseUrl}/api/schedule/${jobId}`, schedule)
}
