<template>
  <div>
    <h2>Schedule</h2>
    <div v-if="!$config.jobSchedulingEnabled" class="d-flex mb-5 ml-3 pb-5 pt-3">
      <span v-if="!$config.jobSchedulingEnabled" class="pr-3">
        <b-icon icon="exclamation-triangle-fill" scale="2" variant="warning" />
      </span>
      <span class="font-weight-bolder text-secondary">Job scheduling is not enabled</span>
    </div>
    <div v-if="$config.jobSchedulingEnabled">
      <LargeSpinner v-if="loading" />
      <div v-if="!loading">
        <b-button variant="info" class="mb-1" @click="reloadSchedule">Reload from configs</b-button>
        <b-card
          v-for="job in jobs"
          :key="job.id"
          no-body
          class="mb-1"
        >
          <b-card-header header-tag="header" class="p-1" role="tab">
            <b-btn
              v-b-toggle="job.id"
              block
              href="#"
              variant="info"
            >
              {{ job.id }}
            </b-btn>
          </b-card-header>
          <b-collapse
            :id="job.id"
            visible
            accordion="job-panel"
            role="tabpanel"
          >
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
                  :key="component"
                >
                  <span class="message">{{ component }}</span>
                </div>
              </div>
              <div v-if="job.args">
                <h3>Args</h3>
                <div
                  v-for="arg in job.args"
                  :key="arg"
                >
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
  </div>
</template>

<script>
import Context from '@/mixins/Context'
import LargeSpinner from '@/components/widgets/LargeSpinner'
import {getSchedule, reloadSchedule, removeSchedule, updateSchedule} from '@/api/schedule'

export default {
  name: 'Schedule',
  mixins: [Context],
  components: {LargeSpinner},
  data: () => ({
    jobs: []
  }),
  created() {
    this.getSchedule()
  },
  methods: {
    getSchedule() {
      if (this.$config.jobSchedulingEnabled) {
        this.$loading()
        getSchedule().then(data => {
          this.jobs = data
          this.$ready()
        })
      } else {
        this.$ready()
      }
    },
    pauseJob(jobId) {
      this.$loading()
      updateSchedule(jobId, {}).then(updatedJob => {
        this.jobs.forEach((job, index) => {
          if (job.id === updatedJob.id) {
            this.jobs.splice(index, 1)
          }
        })
        this.jobs.push(updatedJob)
        this.$ready()
      })
    },
    reloadSchedule() {
      this.$loading()
      reloadSchedule().then(data => {
        this.jobs = data
        this.$ready()
      })
    },
    removeJob(jobId) {
      this.$loading()
      removeSchedule(jobId).then(data => {
        this.jobs = data
        this.$ready()
      })
    },
  }
}
</script>

<style scoped>
h2 {
  color: #17a2b8;
  padding-top: 20px;
}
</style>
