<template>
  <div>
    <h2>
      {{ job.id }}
    </h2>
    <div>
      <button
        @click="startJob"
        @click.prevent="!!jobStatus">Start Job</button>
    </div>
    <div v-if="jobStatus">
      <b>Job Status: </b> {{ jobStatus }}
    </div>
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
  </div>
</template>

<script>
import { startJob } from '@/api/job';
import { getJobSchedule } from '@/api/schedule';

export default {
  data() {
    return {
      job: {},
      jobStatus: null
    };
  },
  created() {
    this.getJob(this.$route.params.id);
  },
  methods: {
    getJob(jobId) {
      getJobSchedule(jobId)
        .then(data => {
          this.job = data;
        })
        .catch(error => console.log(error)) // eslint-disable-line no-console
        .finally(() => {
          this.loading = false;
        });
    },
    startJob() {
      startJob(this.job.id)
        .then(data => {
          this.jobStatus = data;
        })
        .catch(error => console.log(error)) // eslint-disable-line no-console
        .finally(() => {
          this.loading = false;
        });
    }
  }
};
</script>
