import axios from 'axios'
import store from '@/store'

export function create8BallSchedule(schedule: object) {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .post(`${apiBaseUrl}/api/8ball/schedules`, schedule)
    .then(response => response.data, err => err.response)
}

export function delete8BallSchedule(scheduleId: string) {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .delete(`${apiBaseUrl}/api/8ball/schedules/${scheduleId}`)
    .then(response => response.data, err => err.response)
}

export function get8BallSchedules() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/8ball/schedules`)
    .then(response => response.data, err => err.response)
}

export function update8BallSchedule(scheduleId: string, schedule: object) {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .post(`${apiBaseUrl}/api/8ball/schedules/${scheduleId}`, schedule)
    .then(response => response.data, err => err.response)
}
