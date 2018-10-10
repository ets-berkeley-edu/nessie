import axios from "axios";
import store from "@/store";

export default {
  getJobSchedule(jobId) {
    return axios
      .get(`${store.state.apiBaseURL}/api/schedule/${jobId}`)
      .then(response => response.data);
  },
  getSchedule() {
    return axios
      .get(`${store.state.apiBaseURL}/api/schedule`)
      .then(response => response.data);
  }
};
