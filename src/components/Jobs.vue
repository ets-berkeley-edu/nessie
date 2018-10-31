<template>
  <div>
    <h2>Jobs</h2>
    <div>
      <label for="runnable-jobs-select">Select a job to run:</label>
      <select id="runnable-jobs-select" v-model="runnableJob.selected">
        <option v-for="option in runnableJobs"
                v-bind:value="option"
                v-bind:key="option.path">{{ option.name }}</option>
      </select>
      <div v-if="runnableJob.selected">
        <div v-if="runnableJob.selected.requiredParameters.length">
          <div v-for="(requiredParameter, index) in runnableJob.selected.requiredParameters" :key="requiredParameter">
            {{requiredParameter}}: <input v-model="runnableJob.params[index]" placeholder="Required">
          </div>
        </div>
        <button @click="runSelectedJob" :disabled="!runnableJob.ready()">Run it!</button>
      </div>
      <div>
        More info: {{ runnableJob }}
      </div>
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
import { getBackgroundJobStatus } from '@/api/job';
import Datepicker from 'vuejs-datepicker';
import store from '@/store';

export default {
  name: 'Jobs',
  components: {
    Datepicker
  },
  computed: {
    runnableJobs() {
      return store.getters.runnableJobs;
    }
  },
  created() {
    this.getBackgroundJobStatus();
  },
  data() {
    return {
      jobsDate: new Date(),
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
      },
      runnableJob: {
        selected: null,
        params: [],
        ready: () => {
          let option = this.runnableJob.selected;
          let nonEmptyParams = this.runnableJob.params.filter(p => p.length);
          return (
            !!option &&
            nonEmptyParams.length === option.requiredParameters.length
          );
        }
      }
    };
  },
  methods: {
    getBackgroundJobStatus() {
      this.jobStatuses.loading = true;
      getBackgroundJobStatus(this.jobsDate).then(data => {
        this.jobStatuses.rows = data;
        this.jobStatuses.loading = false;
      });
    },
    runSelectedJob() {
      console.log('Run job: ' + this.runnableJob.selected.name);
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
