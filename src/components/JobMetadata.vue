<template>
  <div>
    <h2>Failures from last sync: {{ lastSyncJobId }}</h2>
    <div v-if="!failuresFromLastSync.length">
      None
    </div>
    <div v-for="failure in failuresFromLastSync" :key="failure">
      {{ failure }}
    </div>
    <h2>Background Jobs of <datepicker placeholder="Select Date"
                                       v-model="jobsDate"
                                       @disabled="jobStatuses.loading"
                                       @closed="getBackgroundJobStatus"></datepicker></h2>

    <div v-if="jobStatuses.loading">
      <i class="fas fa-sync fa-spin fa-5x"></i>
      <span role="alert" aria-live="passive" class="sr-only">Loading...</span>
    </div>
    <b-table striped
             hover
             :items="jobStatuses.rows"
             :fields="jobStatuses.fields"
             v-if="!jobStatuses.loading"></b-table>
  </div>
</template>

<script>
import { getBackgroundJobStatus, getFailuresFromLastSync } from '@/api/job';
import Datepicker from 'vuejs-datepicker';

export default {
  name: 'JobMetadata',
  components: {
    Datepicker
  },
  created() {
    this.getFailuresFromLastSync();
    this.getBackgroundJobStatus();
  },
  data() {
    return {
      jobsDate: new Date(),
      error: null,
      lastSyncJobId: null,
      failuresFromLastSync: [],
      jobStatuses: {
        rows: [],
        fields: [
          { key: 'id', sortable: true },
          { key: 'status', sortable: true },
          { key: 'details' },
          { key: 'started', sortable: true },
          { key: 'finished', sortable: true }
        ],
        loading: true
      }
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
    getBackgroundJobStatus() {
      this.jobStatuses.loading = true;
      getBackgroundJobStatus(this.jobsDate)
        .then(data => {
          this.jobStatuses.rows = data;
          this.jobStatuses.loading = false;
        })
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
