import axios from "axios";
import store from "@/store";

export default {
  getCasLoginURL() {
    return axios
      .get(`${store.state.apiBaseURL}/api/user/cas_login_url`)
      .then(response => response.data.casLoginURL);
  },
  getMyProfile() {
    return axios.get(`${store.state.apiBaseURL}/api/user/profile`);
  },
  logOut() {
    return axios.get(`${store.state.apiBaseURL}/api/user/logout`);
  }
};
