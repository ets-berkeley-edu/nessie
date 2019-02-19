<template>
  <div>
    <h1>Manage Jobs</h1>
    <div v-if="!runnableJobs">
      Sorry, no runnable jobs were found.
    </div>
    <div v-if="runnableJobs">
      <div class="flex-row">
        <div>
          <b-form-select v-model="selected" class="mb-3">
            <option :value="null">Select...</option>
            <option
              v-for="job in runnableJobs"
              :key="job.id"
              :value="job">
              {{ job.name }}
            </option>
          </b-form-select>
        </div>
        <div v-if="selected">
          <div v-for="key in selected.required" :key="key" class="job-params">
            {{ key }}:
            <input v-model="params[key]" />
          </div>
        </div>
        <div>
          <b-button variant="success" :disabled="!selectedJobRunnable" @click="runSelectedJob">Run</b-button>
        </div>
      </div>
      <div v-if="errored.length">
        <h3>Jobs Errored</h3>
        <ul>
          <li
            v-for="job in errored"
            :key="job.id">
            {{ job.name }}
          </li>
        </ul>
      </div>
      <div v-if="started.length">
        <h3>Jobs Started</h3>
        <ul>
          <li
            v-for="job in started"
            :key="job.id">
            {{ job.name }}
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import store from '@/store';
import { runJob } from '@/api/job';

export default {
  name: 'RunJob',
  data() {
    return {
      errored: [],
      params: {},
      started: [],
      selected: null
    };
  },
  computed: {
    runnableJobs() {
      return store.getters['schedule/runnableJobs'];
    },
    selectedJobRunnable() {
      if (!this.selected) {
        return false;
      }
      return !_.find(this.selected.required, key => !this.params[key]);
    }
  },
  methods: {
    /* eslint no-undef: "warn" */
    runSelectedJob() {
      let apiPath = this.selected.path;
      _.each(this.selected.required, key => {
        apiPath = _.replace(apiPath, '<' + key + '>', this.params[key]);
      });
      runJob(apiPath).then(data => {
        let job = _.remove(this.runnableJobs, this.selected)[0];
        if (data.status.includes('error')) {
          this.errored.push(job);
        } else {
          this.started.push(job);
        }
      });
    }
  }
};
</script>

<style scoped>
.job-params {
  margin: 0 10px;
}
</style>
