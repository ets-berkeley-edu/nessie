<template>
  <div>
    <h2>
      {{ job.id }}
    </h2>
    <div>
      <button @click="startJob" @click.prevent="!!jobStatus">Start Job</button>
    </div>
    <div v-if="jobStatus">
      <b>Job Status: </b> {{jobStatus}}
    </div>
    <div>
      <b>Trigger:</b> {{job.trigger}}
    </div>
    <div>
      <b>Next run:</b> {{job.nextRun}}
    </div>
    <div>
      <b>Locked:</b> {{job.locked}}
    </div>
    <div v-if="job.components">
      <h3>Components</h3>
      <div v-for="component in job.components" :key="component">
        <span class="message">{{component}}</span>
      </div>
    </div>
    <div v-if="job.args">
      <h3>Args</h3>
      <div v-for="arg in job.args" :key="arg">
        <span class="message">{{arg}}</span>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  created() {
    this.getJob(this.$route.params.id);
  },
  data() {
    return {
      job: {},
      jobStatus: null,
    };
  },
  methods: {
    getJob(jobId) {
      axios.get(`/api/schedule/${jobId}`).then((response) => {
        this.job = response.data;
      }).catch((error) => {
        // TODO: What is error handling strategy?
        console.log(error); // eslint-disable-line no-console
      });
    },
    startJob() {
      axios.post(`/api/job/${this.job.id}`).then((response) => {
        this.jobStatus = response.data;
      }).catch((error) => {
        console.log(error); // eslint-disable-line no-console
      });
    },
  },
};
</script>
