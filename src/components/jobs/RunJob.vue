<template>
  <div>
    <h1>Manage Jobs</h1>
    <div v-if="!runnableJobs">
      Sorry, no runnable jobs were found.
    </div>
    <div v-if="runnableJobs">
      <div class="flex-row">
        <div>
          <b-form-select class="mb-3" v-model="selected">
            <option :value="null">Select...</option>
            <option :value="job"
                    v-for="job in runnableJobs"
                    v-bind:key="job.id">{{ job.name }}</option>
          </b-form-select>
        </div>
        <div>
          <b-button @click="runSelectedJob" variant="success">Run</b-button>
        </div>
      </div>
      <div v-if="errored.length">
        <h3>Jobs Errored</h3>
        <ul>
          <li v-for="job in errored"
              v-bind:key="job.id">{{ job.name }}</li>
        </ul>
      </div>
      <div v-if="started.length">
        <h3>Jobs Started</h3>
        <ul>
          <li v-for="job in started"
              v-bind:key="job.id">{{ job.name }}</li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
import { runJob } from '@/api/job';
import store from '@/store';

export default {
  name: 'RunJob',
  computed: {
    runnableJobs() {
      return store.getters.runnableJobs;
    }
  },
  data() {
    return {
      errored: [],
      started: [],
      selected: null
    };
  },
  methods: {
    /* eslint no-undef: "warn" */
    runSelectedJob() {
      let apiPath = this.selected.path;
      _.each(this.arguments.required, key => {
        apiPath = _.replace(
          apiPath,
          '<' + key + '>',
          this.arguments.required[key]
        );
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
