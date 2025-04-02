import axios from 'axios'
import {useContextStore} from '@/stores/context'

export function create8BallSchedule(schedule: object) {
  return axios.post(`${useContextStore().config.apiBaseUrl}/api/8ball/schedules`, schedule)
}

export function delete8BallSchedule(scheduleId: string) {
  return axios.delete(`${useContextStore().config.apiBaseUrl}/api/8ball/schedules/${scheduleId}`)
}

export function get8BallSchedules() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/8ball/schedules`)
}

export function update8BallSchedule(scheduleId: string, schedule: object) {
  return axios.post(`${useContextStore().config.apiBaseUrl}/api/8ball/schedules/${scheduleId}`, schedule)
}
