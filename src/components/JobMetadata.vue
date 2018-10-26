<template>
  <div>
    <p class="failed" v-if="error" v-bind="error"></p>
    <h2>Failures from last sync: {{ lastSyncJobId }}</h2>
    <div v-if="!failuresFromLastSync.length">
      None
    </div>
    <div v-for="failure in failuresFromLastSync" :key="failure">
      {{ failure }}
    </div>
    <h2>Background Jobs of {{ date.toLocaleDateString() }}</h2>
    <b-table striped hover :items="jobStatuses"></b-table>
  </div>
</template>

<script>
import { getBackgroundJobStatus, getFailuresFromLastSync } from '@/api/job';

export default {
  name: 'JobMetadata',
  created() {
    this.getFailuresFromLastSync();
    this.getJobStatusesFromToday();
  },
  data() {
    return {
      date: new Date(),
      error: null,
      lastSyncJobId: null,
      failuresFromLastSync: [],
      jobStatuses: []
    };
  },
  methods: {
    getFailuresFromLastSync() {
      getFailuresFromLastSync()
        .then(data => {
          this.lastSyncJobId = data.jobId;
          this.failuresFromLastSync = data.failures;
        })
        .catch(err => (this.error = err.message));
    },
    getJobStatusesFromToday() {
      getBackgroundJobStatus(this.date)
        .then(data => (this.jobStatuses = data))
        .catch(err => (this.error = err.message));
    }
  }
};
</script>

<style scoped lang="scss">
td {
  padding-left: 20px;
  vertical-align: top;
}
.succeeded {
  color: green;
}
.failed {
  color: red;
}
.started {
  color: blue;
}
h2 {
  color: #17a2b8;
  padding-top: 20px;
}
</style>
