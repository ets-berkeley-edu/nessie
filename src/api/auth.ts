import axios from 'axios'
import {useContextStore} from '@/stores/context'

export function getCasLoginURL() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/user/cas_login_url`)
}

export function getCasLogoutURL() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/user/cas_logout_url`)
}
