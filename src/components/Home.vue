<template>
  <div>
    <p>Nessie</p>
    <div v-if="loading">Loading...</div>
    <div v-if="!loading">
      <button @click="getSchedule">Refresh</button>
      <div v-for="job in jobs" v-bind:key="job.id">
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
  </div>
</template>

<script>
import ScheduleApi from '@/services/api/ScheduleApi.js';

export default {
  created() {
    this.getSchedule();
  },
  data() {
    return {
      jobs: [],
      loading: true,
    };
  },
  methods: {
    getSchedule() {
      this.loading = true;
      ScheduleApi.getSchedule()
        .then((data) => { this.jobs = data; })
        .catch(error => console.log(error)) // eslint-disable-line no-console
        .finally(() => { this.loading = false; });
    },
  },
};
</script>
