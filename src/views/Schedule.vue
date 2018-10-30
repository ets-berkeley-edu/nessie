<template>
  <div>
    <h2>Schedule</h2>
    <b-card no-body class="mb-1" v-for="job in jobs" :key="job.id">
      <b-card-header header-tag="header" class="p-1" role="tab">
        <b-btn block href="#" v-b-toggle=job.id variant="info">{{ job.id }}</b-btn>
      </b-card-header>
      <b-collapse :id="job.id" visible accordion="job-panel" role="tabpanel">
        <b-card-body>
          <div>
            <b>Trigger:</b> {{ job.trigger }}
          </div>
          <div>
            <b>Next run:</b> {{ job.nextRun }}
          </div>
          <div>
            <b>Locked:</b> {{ job.locked }}
          </div>
          <div v-if="job.components">
            <h3>Components</h3>
            <div
              v-for="component in job.components"
              :key="component">
              <span class="message">{{ component }}</span>
            </div>
          </div>
          <div v-if="job.args">
            <h3>Args</h3>
            <div
              v-for="arg in job.args"
              :key="arg">
              <span class="message">{{ arg }}</span>
            </div>
          </div>
        </b-card-body>
      </b-collapse>
    </b-card>
  </div>
</template>

<script>
import { getSchedule } from '@/api/schedule';

export default {
  name: 'Schedule',
  created() {
    this.getSchedule();
  },
  data() {
    return {
      jobs: [],
      loading: true
    };
  },
  methods: {
    getSchedule() {
      getSchedule().then(data => (this.jobs = data));
    }
  }
};
</script>

<style scoped lang="scss">
h2 {
  color: #17a2b8;
  padding-top: 20px;
}
</style>
