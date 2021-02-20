import axios from 'axios'
import store from '@/store'

export function getConfig() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/config`)
    .then(response => response.data, err => err.response)
}

export function getPing() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/ping`)
    .then(response => response.data, err => err.response)
}

export function getVersion() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/version`)
    .then(response => response.data, err => err.response)
}

export function getXkcd() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/admin/xkcd`)
    .then(response => response.data, err => err.response)
}
