<template>
  <div>
    <p>Nessie</p>
    <button @click="getSchedule">Refresh</button>
    <div v-for="job in jobs" :key="job.id">
      <h2>
        <router-link :to="{name: 'Job', params: { id: job.id }}">{{job.id}}</router-link>
      </h2>
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
  </div>
</template>

<script>
import axios from 'axios';

export default {
  created() {
    this.getSchedule();
  },
  data() {
    return {
      jobs: [],
    };
  },
  methods: {
    getSchedule() {
      axios.get('/api/schedule').then((response) => {
        this.jobs = response.data;
      }).catch(((error) => {
        console.log(error); // eslint-disable-line no-console
      }));
    },
  },
};
</script>
