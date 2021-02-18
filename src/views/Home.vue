<template>
  <div v-if="!loading" class="mx-2">
    <div v-if="!runnableJobs.length">
      Sorry, no runnable jobs were found.
    </div>
    <div v-if="runnableJobs.length" class="mb-4 py-3">
      <div class="align-items-start d-flex justify-content-between">
        <div class="align-items-start d-flex">
          <div class="pr-2">
            <b-form-select id="select-job" v-model="selected">
              <option :value="null">Select job...</option>
              <option
                v-for="job in runnableJobs"
                :key="job.id"
                :value="job"
              >
                {{ job.name }}
              </option>
            </b-form-select>
            <div v-if="$_.get(selected, 'required.length')" class="pl-2 pt-2">
              <div v-for="key in selected.required" :key="key" class="text-secondary">
                <span class="pb-1 pr-1">{{ $_.capitalize(key) }}:</span>
                <input v-model="params[key]" />
              </div>
            </div>
          </div>
          <div class="pr-2">
            <b-button variant="success" :disabled="!selectedJob" @click="runSelectedJob">Run</b-button>
          </div>
        </div>
        <div class="align-items-center d-flex mr-5 pr-3">
          <div class="pr-2 pt-1 text-secondary">
            Showing jobs run on
          </div>
          <div class="pr-2 pt-1">
            <Datepicker
              v-model="jobsDate"
              :disabled-dates="{
                from: new Date(),
                to: new Date(2019, 1, 28)
              }"
              input-class="jobs-datepicker"
              placeholder="Select Date"
              @disabled="!!loading"
              @closed="getBackgroundJobStatus"
            ></Datepicker>
          </div>
          <div class="d-flex">
            <div class="pr-2">
              <b-badge pill variant="danger"><span class="config-pill">{{ errored.length }}</span></b-badge>
            </div>
            <div class="pr-2">
              <b-badge pill variant="warning"><span class="config-pill">{{ started.length }}</span></b-badge>
            </div>
            <div>
              <b-badge pill variant="success"><span class="config-pill">{{ jobStatuses.rows.length - (errored.length + started.length) }}</span></b-badge>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div>
      <div class="results-container">
        <LargeSpinner v-if="loading" />
        <div v-if="!loading">
          <div v-if="jobStatuses.rows.length">
            <b-table
              striped
              hover
              :items="jobStatuses.rows"
              :fields="jobStatuses.fields"
            ></b-table>
          </div>
          <div v-if="!jobStatuses.rows.length" class="font-weight-bolder ml-2 my-5 text-secondary">
            No jobs run on {{ jobsDate | moment("dddd, MMMM Do, YYYY") }} (UTC).
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Context from '@/mixins/Context'
import Datepicker from 'vuejs-datepicker'
import LargeSpinner from '@/components/widgets/LargeSpinner'
import {getBackgroundJobStatus, getRunnableJobs, runJob} from '@/api/job'

export default {
  name: 'Home',
  mixins: [Context],
  components: {
    Datepicker,
    LargeSpinner
  },
  data: () => ({
    errored: [],
    jobsDate: new Date(),
    jobStatuses: {
      rows: [],
      fields: [
        { key: 'id', sortable: true },
        { key: 'status', sortable: true },
        { key: 'details'},
        { key: 'started', sortable: true, class: 'text-nowrap' },
        { key: 'finished', sortable: true, class: 'text-nowrap' }
      ]
    },
    params: {},
    runnableJobs: [],
    started: [],
    selected: null,
  }),
  computed: {
    selectedJob() {
      if (!this.selected) {
        return false
      }
      return !this.$_.find(this.selected.required, key => !this.params[key])
    }
  },
  created() {
    this.$loading()
    getRunnableJobs().then(data => {
      this.runnableJobs = data
      this.getBackgroundJobStatus()
      this.$ready()
    })
  },
  methods: {
    getBackgroundJobStatus() {
      this.$loading()
      getBackgroundJobStatus(this.jobsDate).then(data => {
        this.errored = []
        this.started = []
        this.jobStatuses.rows = this.$_.map(data, row => {
          let style = 'success'
          if (row.status === 'failed') {
            style = 'danger'
            this.errored.push(row)
          } else if (row.status === 'started') {
            style = 'info'
            this.started.push(row)
          } else {
            style = 'success'
          }
          row._cellVariants = { status: style }
          return row
        })
        this.$ready()
      })
    },
    /* eslint no-undef: "warn" */
    runSelectedJob() {
      let apiPath = this.selected.path
      this.$_.each(this.selected.required, key => {
        apiPath = this.$_.replace(apiPath, '<' + key + '>', this.params[key])
      })
      runJob(apiPath).then(data => {
        let job = this.$_.remove(this.runnableJobs, this.selected)[0]
        if (data.status.includes('error')) {
          this.errored.push(job)
        } else {
          this.started.push(job)
        }
      })
    }
  }
}
</script>

<style>
.jobs-datepicker {
  text-align: center;
  width: 110px;
}
</style>
