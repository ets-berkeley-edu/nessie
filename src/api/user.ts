import axios from 'axios';
import store from '@/store';

export function getCasLoginURL() {
  return axios
    .get(`${store.state.apiBaseURL}/api/user/cas_login_url`)
    .then(response => response.data);
}

export function getMyProfile() {
  console.log(`${store.state.apiBaseURL}/api/user/profile`);
  return axios
    .get(`${store.state.apiBaseURL}/api/user/profile`)
    .then(response => response.data);
}

export function getCasLogoutURL() {
  return axios
    .get(`${store.state.apiBaseURL}/api/user/cas_logout_url`)
    .then(response => response.data);
}
