import axios from 'axios'
import store from '@/store'

export function getCasLoginURL() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/user/cas_login_url`)
    .then(response => response.data, err => err.response)
}

export function getCasLogoutURL() {
  const apiBaseUrl = store.getters['context/apiBaseUrl']
  return axios
    .get(`${apiBaseUrl}/api/user/cas_logout_url`)
    .then(response => response.data, err => err.response)
}
