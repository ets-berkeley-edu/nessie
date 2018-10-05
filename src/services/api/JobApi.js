import axios from "axios";
import store from "@/store";

export default {
  startJob(jobId) {
    return axios
      .post(`${store.state.apiBaseURL}/api/job/${jobId}`)
      .then(response => response.data);
  }
};
