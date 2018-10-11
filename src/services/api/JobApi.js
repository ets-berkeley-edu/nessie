import axios from 'axios';

export default {
  startJob(jobId) {
    return axios.post(`${process.env.VUE_APP_API_BASE_URL}/api/job/${jobId}`)
      .then(response => response.data);
  },
};
