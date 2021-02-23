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
                :disabled="!isAvailable(job) || starting"
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
            <b-button variant="success" :disabled="!selectedJob || starting" @click="run">Run</b-button>
          </div>
        </div>
        <div class="align-items-center d-flex mr-5 pr-3">
          <div class="pr-2 pt-1 text-secondary">
            Showing jobs run on
          </div>
          <div class="pr-2 pt-1">
            <Datepicker
              v-model="dateSelected"
              :disabled-dates="{
                from: new Date(),
                to: new Date(2019, 1, 28)
              }"
              input-class="jobs-datepicker"
              placeholder="Select Date"
              @disabled="!!loading"
              @closed="refresh"
            ></Datepicker>
          </div>
          <div class="d-flex">
            <div class="pr-2">
              <b-badge pill variant="danger"><span class="config-pill">{{ jobs.errored.length }}</span></b-badge>
            </div>
            <div class="pr-2">
              <b-badge pill variant="warning"><span class="config-pill">{{ jobs.started.length }}</span></b-badge>
            </div>
            <div>
              <b-badge pill variant="success"><span class="config-pill">{{ jobs.all.length - (jobs.errored.length + jobs.started.length) }}</span></b-badge>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div>
      <div class="results-container">
        <LargeSpinner v-if="loading" />
        <div v-if="!loading">
          <div v-if="jobs.all.length">
            <b-table
              striped
              hover
              :items="jobs.all"
              :fields="jobs.fields"
            >
              <template #cell(status)="data">
                <div class="align-items-center d-flex justify-content-center">
                  <div class="pr-1">
                    {{ data.value }}
                  </div>
                  <div>
                    <b-icon
                      v-if="(data.value === 'started') && !data.item.finished && getAgeInHours(data.item) > 5"
                      v-b-tooltip.hover
                      icon="question-square-fill"
                      variant="danger"
                      :title="`Job has been running for ${getAgeInHours(data.item)} hours. Is it stalled?`"
                    />
                  </div>
                </div>
              </template>
              <template #cell(details)="data">
                <span v-html="data.value"></span>
              </template>
              <template #cell(started)="data">
                {{ data.value | moment("HH:mm:ss") }}
              </template>
              <template #cell(finished)="data">
                <span v-if="data.value">{{ data.value | moment("HH:mm:ss") }}</span>
              </template>
              <template #cell(duration)="data">
                {{ data.value ? describeDuration(data.value) : '&mdash;' }}
              </template>
            </b-table>
          </div>
          <div v-if="!jobs.all.length" class="font-weight-bolder ml-2 my-5 text-secondary">
            No jobs run on {{ dateSelected | moment("dddd, MMMM Do, YYYY") }} (UTC).
          </div>
        </div>
      </div>
    </div>
    <b-toast
      v-if="alert"
      v-model="starting"
      :title="alert.title"
      toaster="b-toaster-top-full"
      variant="success"
    >
      <template #toast-title>
        <div class="d-flex align-items-center">
          <div class="px-3">
            <b-icon icon="three-dots" animation="cylon" font-scale="2"></b-icon>
          </div>
          <h3>{{ alert.title }}</h3>
        </div>
      </template>
      <div class="my-4 text-center">
        <img :alt="$_.get(alert, 'xkcd.alt')" :src="$_.get(alert, 'xkcd.img')" />
      </div>
    </b-toast>
  </div>
</template>

<script>
import Context from '@/mixins/Context'
import Datepicker from 'vuejs-datepicker'
import LargeSpinner from '@/components/widgets/LargeSpinner'
import {getBackgroundJobStatus, getRunnableJobs, runJob} from '@/api/job'
import {getXkcd} from '@/api/status'

export default {
  name: 'Home',
  mixins: [Context],
  components: {
    Datepicker,
    LargeSpinner
  },
  data: () => ({
    alert: undefined,
    dateSelected: new Date(),
    jobs: {
      all: [],
      errored: [],
      fields: [
        {key: 'id', label: 'Job', sortable: true},
        {key: 'status', sortable: true, class: 'text-center'},
        {key: 'details', label: 'Summary'},
        {key: 'started', label: 'Start (UTC)', sortable: true, class: 'text-nowrap text-right'},
        {key: 'finished', label: 'End (UTC)', sortable: true, class: 'text-nowrap text-right'},
        {key: 'duration', label: 'Duration (h:mm:ss)', sortable: true, class: 'text-nowrap text-right'}
      ],
      started: []
    },
    params: {},
    runnableJobs: [],
    selected: null,
    starting: false
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
      this.refresh().then(this.$ready)
    })
  },
  methods: {
    describeDuration(ms) {
      const d = this.$moment.duration(ms / 1000, 'seconds')
      const pad = n => `${n < 10 ? '0' : ''}${n}`
      return `${pad(d.hours())}:${pad(d.minutes())}:${pad(d.seconds())}`
    },
    getAgeInHours(job) {
      const started = this.$moment.utc(job.started)
      return Math.floor(this.$moment.utc().diff(started) / (1000 * 60 * 60))
    },
    isAvailable(job) {
      let available = true
      this.$_.each(this.jobs.started, started => {
        const id = this.$_.split(started.id, '_')[0].toLowerCase()
        const name = job.name.toLowerCase().replace(/\s/g, '')
        if (id === name) {
          available = false
          return false
        }
      })
      return available
    },
    refresh() {
      return getBackgroundJobStatus(this.dateSelected).then(jobs => {
        this.jobs.errored = []
        this.jobs.started = []
        this.jobs.all = this.$_.map(jobs, job => {
          let style
          if (job.status === 'failed') {
            style = 'danger'
            this.jobs.errored.push(job)
          } else if (job.status === 'started') {
            style = 'info'
            this.jobs.started.push(job)
          } else {
            style = 'success'
          }
          job._cellVariants = {status: style}
          const finished = job.finished && this.$moment.utc(job.finished)
          job.duration = finished && finished.diff(this.$moment.utc(job.started))
          return job
        })
      })
    },
    run() {
      getXkcd().then(xkcd => {
        this.alert = {
          startedAt: new Date(),
          title: `${this.selected.name} started`,
          xkcd
        }
        this.starting = true
        let apiPath = this.selected.path
        this.$_.each(this.selected.required, key => {
          apiPath = this.$_.replace(apiPath, `<${key}>`, this.params[key])
        })
        runJob(apiPath).then(() => {
          this.selected = null
          this.refresh()
          setTimeout(() => (this.starting = false), 30000)
        })
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
