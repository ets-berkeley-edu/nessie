import axios from 'axios';

export default {
  getJobSchedule(jobId) {
    return axios.get(`${process.env.VUE_APP_API_BASE_URL}/api/schedule/${jobId}`)
      .then(response => response.data);
  },
  getSchedule() {
    return axios.get(`${process.env.VUE_APP_API_BASE_URL}/api/schedule`)
      .then(response => response.data);
  },
};
