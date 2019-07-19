<template>
  <div>
    <h2>Schedule</h2>
    <LargeSpinner v-if="loading" />
    <div v-if="!loading">
      <b-button variant="info" class="mb-1" @click="reloadSchedule">Reload from configs</b-button>
      <b-card
        v-for="job in jobs"
        :key="job.id"
        no-body
        class="mb-1">
        <b-card-header header-tag="header" class="p-1" role="tab">
          <b-btn
            v-b-toggle="job.id"
            block
            href="#"
            variant="info">
            {{ job.id }}
          </b-btn>
        </b-card-header>
        <b-collapse
          :id="job.id"
          visible
          accordion="job-panel"
          role="tabpanel">
          <b-card-body>
            <div>
              <b>Trigger:</b> {{ job.trigger }}
            </div>
            <div>
              <b>Next run:</b> {{ job.nextRun | moment('calendar') }}
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
            <b-button-group class="mt-3">
              <b-button variant="outline-secondary" @click="pauseJob(job.id)">Pause</b-button>
              <b-button variant="outline-danger" @click="removeJob(job.id)">Remove</b-button>
            </b-button-group>
          </b-card-body>
        </b-collapse>
      </b-card>
    </div>
  </div>
</template>

<script>
import { getSchedule, reloadSchedule, removeSchedule, updateSchedule } from '@/api/schedule';
import LargeSpinner from '@/components/widgets/LargeSpinner';

export default {
  name: 'Schedule',
  components: {
    LargeSpinner
  },
  data() {
    return {
      jobs: [],
      loading: true
    };
  },
  created() {
    this.getSchedule();
  },
  methods: {
    getSchedule() {
      this.loading = true;
      getSchedule().then(data => {
        this.jobs = data;
        this.loading = false;
      });
    },
    pauseJob(jobId) {
      this.loading = true;
      updateSchedule(jobId, {}).then(updatedJob => {
        this.jobs.forEach((job, index) => {
          if (job.id === updatedJob.id) {
            this.jobs.splice(index, 1);
          }
        });
        this.jobs.push(updatedJob);
        this.loading = false;
      });
    },
    reloadSchedule() {
      this.loading = true;
      reloadSchedule().then(data => {
        this.jobs = data;
        this.loading = false;
      });
    },
    removeJob(jobId) {
      this.loading = true;
      removeSchedule(jobId).then(data => {
        this.jobs = data;
        this.loading = false;
      });
    },
  }
};
</script>

<style scoped>
h2 {
  color: #17a2b8;
  padding-top: 20px;
}
</style>
