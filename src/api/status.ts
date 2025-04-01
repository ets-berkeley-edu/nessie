import axios from 'axios'
import {useContextStore} from '@/stores/context'

export function getConfig() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/config`)
}

export function getVersion() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/version`)
}

export function getXkcd() {
  return axios.get(`${useContextStore().config.apiBaseUrl}/api/admin/xkcd`)
}
