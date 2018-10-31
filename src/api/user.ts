import axios from 'axios';
import store from '@/store';

export function getCasLoginURL() {
  return axios
    .get(`${store.state.apiBaseURL}/api/user/cas_login_url`)
    .then(response => response.data, err => err.response);
}

export function getMyProfile() {
  return axios
    .get(`${store.state.apiBaseURL}/api/user/profile`)
    .then(response => response.data, err => err.response);
}

export function getCasLogoutURL() {
  return axios
    .get(`${store.state.apiBaseURL}/api/user/cas_logout_url`)
    .then(response => response.data, err => err.response);
}
