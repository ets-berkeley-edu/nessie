<template>
  <div v-if="!contextStore.isLoading" class="mx-2">
    <div v-if="!runnableJobs.length">
      Sorry, no runnable jobs were found.
    </div>
    <v-row v-if="runnableJobs.length" class="mb-4 py-3" align="center">
      <v-col class="pr-2">
        <v-select
          id="select-job"
          v-model="selected"
          :items="runnableJobs"
          item-title="name"
          :item-props="true"
          placeholder="Select job..."
          hide-details
          return-object
        >
          <template #item="{ props: itemProps }">
            <v-list-item v-bind="itemProps" :disabled="!isAvailable(itemProps) || starting"></v-list-item>
          </template>
        </v-select>
        <div v-if="get(selected, 'required.length')" class="pl-2 pt-2">
          <div v-for="key in selected.required" :key="key" class="d-flex align-center">
            <span class="pb-1 pr-1">{{ capitalize(key) }}:</span>
            <v-text-field v-model="params[key]" hide-details />
          </div>
        </div>
      </v-col>
      <v-col class="pr-2">
        <v-btn :disabled="!isJobSpecified || starting" @click="run">Run</v-btn>
      </v-col>
      <v-col class="pr-0 text-right">
        Showing jobs run on
      </v-col>
      <v-col>
        <v-date-input
          v-model="dateSelected"
          placeholder="Select Date"
          :disabled="contextStore.isLoading"
          hide-actions
          hide-details
          @update:modelValue="refresh"
        ></v-date-input>
      </v-col>
      <v-col class="flex-grow-1">
        <v-badge inline color="red" :content="jobs.errored.length" />
        <v-badge inline color="yellow" :content="jobs.started.length" />
        <v-badge inline color="green" :content="jobs.all.length - (jobs.errored.length + jobs.started.length)" />
      </v-col>
    </v-row>
    <div>
      <div class="results-container">
        <LargeSpinner v-if="contextStore.isLoading" />
        <div v-if="!contextStore.isLoading">
          <div v-if="jobs.all.length" class="striped-table">
            <v-alert v-if="jobs.started.length === 1" show>
              { jobs.started[0].id }} is running.
            </v-alert>
            <v-alert v-if="jobs.started.length > 1" show>
              <h2 class="jobs-running-header">Jobs Running</h2>
              <ul>
                <li v-for="job in jobs.started" :key="job.id">{{ job.id }}</li>
              </ul>
            </v-alert>
            <v-data-table
              :headers="headers"
              :items="jobs.all"
              items-per-page="-1"
              hide-default-footer
              :mobile="xs"
            >
              <template #item.id="{ item }">
                {{ item.id.split('_')[0] }}
              </template>
              <template #item.status="{ item }">
                <div class="d-flex justify-center">
                  <div>
                    {{ item.status }}
                  </div>
                  <v-tooltip
                    v-if="(item.status === 'started') && !item.finished && getAgeInHours(item) > 5"
                    :text="`Job has been running for ${getAgeInHours(item)} hours. Is it stalled?`"
                  >
                    <template #activator="{ props }">
                      <v-icon v-bind="props" :icon="mdiHelpBox" />
                    </template>
                  </v-tooltip>
                </div>
              </template>
              <template #item.details="{ item }">
                <div v-html="item.details"></div>
              </template>
              <template #item.started="{ item }">
                {{ toFormatFromISO(item.started, "HH:mm:ss") }}
              </template>
              <template #item.finished="{ item }">
                <span v-if="item.finished">{{ toFormatFromISO(item.finished, "HH:mm:ss") }}</span>
              </template>
              <template #item.duration="{ item }">
                {{ item.duration ? describeDuration(item.duration) : '&mdash;' }}
              </template>
            </v-data-table>
          </div>
          <div v-if="!jobs.all.length" class="font-weight-bolder ml-2 my-5 text-secondary">
            No jobs run on {{ toFormatFromJsDate(dateSelected, 'MMMM D') }} (UTC).
          </div>
        </div>
      </div>
    </div>
    <v-dialog
      v-if="alert"
      v-model="starting"
      no-auto-hide
      :title="alert.title"
      variant="success"
      @close="closeAlert"
    >
      <v-card>
        <div class="d-flex align-center justify-center">
          <div class="px-3">
            <v-icon :icon="mdiCheck" font-scale="2" />
          </div>
          <h3>{{ alert.title }}</h3>
        </div>
        <div class="my-4 text-center">
          <img
            id="xkcd-comic"
            :alt="get(alert, 'xkcd.alt')"
            :src="get(alert, 'xkcd.img')"
            :title="get(alert, 'xkcd.title')"
          />
        </div>
        <v-divider />
        <v-card-actions>
          <v-btn
            id="close-xkcd-dialog"
            text="Close"
            @click="closeAlert"
          />
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script setup>
import {capitalize, each, find, get, map, replace, split} from 'lodash'
import {DateTime} from 'luxon'
import {mdiCheck, mdiHelpBox} from '@mdi/js'
import {computed, onMounted, onUnmounted, ref} from 'vue'
import {useDisplay} from 'vuetify'

import LargeSpinner from '@/components/widgets/LargeSpinner'

import {getBackgroundJobStatus, getRunnableJobs, runJob} from '@/api/job'
import {getXkcd} from '@/api/status'
import {toFormatFromISO, toFormatFromJsDate} from '@/utils'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()

const alert = ref(undefined)
const dateSelected = ref(new Date())

const {xs} = useDisplay()

const headers = [
  {key: 'id', title: 'Job'},
  {
    key: 'status',
    title: 'Status',
    cellProps: (item) => {
      if (item.value === 'failed') {
        return {class: 'bg-red-lighten-4 text-center'}
      } else if (item.value === 'started') {
        return {class: 'bg-blue-lighten-4'}
      } else {
        return {class: 'bg-green-lighten-4'}
      }
    }
  },
  {key: 'details', title: 'Summary', sortable: false},
  {key: 'started', title: 'Start (UTC)'},
  {key: 'finished', title: 'End (UTC)'},
  {key: 'duration', title: 'Duration (hh:mm:ss)'}
]

const jobs = ref({
  all: [],
  errored: [],
  started: []
})

const pageRefresh = ref(undefined)
const params = ref({})
const runnableJobs = ref([])
const selected = ref(null)
const starting = ref(false)
const toastTimer = ref(undefined)

const isJobSpecified = computed(() => {
  if (!selected.value) {
    return false
  }
  return !find(selected.value.required, key => !params.value[key])
})

onMounted(() => {
  contextStore.loadingStart()
  getRunnableJobs().then(data => {
    runnableJobs.value = data
    refresh().then(contextStore.loadingComplete)
  })
})

onUnmounted(() => clearTimeout(pageRefresh.value))

const closeAlert = () => {
  starting.value = false
  alert.value = null
  clearTimeout(toastTimer.value)
}

const describeDuration = (d) => {
  const pad = n => `${n < 10 ? '0' : ''}${n}`
  return `${pad(d.hours)}:${pad(d.minutes)}:${pad(Math.floor(d.seconds))}`
}

const getAgeInHours = (job) => {
  const started = DateTime.fromISO(job.started)
  return DateTime.utc().diff(started, ['hours']).hours
}

const isAvailable = (job) => {
  let available = true
  each(jobs.value.started, started => {
    const id = split(started.id, '_')[0].toLowerCase()
    const name = job.name.toLowerCase().replace(/\s/g, '')
    if (id === name) {
      available = false
      return false
    }
  })
  return available
}

const refresh = () => {
  clearTimeout(pageRefresh.value)
  const refreshedJobs = {}
  return getBackgroundJobStatus(dateSelected.value).then(data => {
    refreshedJobs.errored = []
    refreshedJobs.started = []
    refreshedJobs.all = map(data, job => {
      const finished = job.finished && DateTime.fromISO(job.finished)
      job.duration = finished && finished.diff(DateTime.fromISO(job.started), ['hours', 'minutes', 'seconds'])
      return job
    })
    jobs.value = refreshedJobs
    contextStore.broadcast('homepage-refresh')
    schedulePageRefresh()
  })
}

const run = () => {
  getXkcd().then(xkcd => {
    alert.value = {
      startedAt: new Date(),
      title: `${selected.value.name} started`,
      xkcd
    }
    starting.value = true
    let apiPath = selected.value.path
    each(selected.value.required, key => {
      apiPath = replace(apiPath, `<${key}>`, params.value[key])
    })
    runJob(apiPath).then(() => {
      selected.value = null
      refresh()
      toastTimer.value = setTimeout(closeAlert.value, 60000)
    })
  })
}

const schedulePageRefresh = () => {
  const interval = 10000 // Milliseconds
  clearTimeout(pageRefresh.value)
  pageRefresh.value = setTimeout(refresh, interval)
}
</script>

<style>
.jobs-running-header {
  font-size: 16px;
}
pre {
  background-color: #f5c6cb;
  border: 1px solid #ccc;
  font-size: 12px;
  margin: 10px 0 10px 0;
  overflow: auto;
  padding: 10px;
  width: auto;
}
.v-data-table__tr pre {
  max-width: 600px;
}
.v-data-table__tr--mobile pre {
  max-width: 400px;
}
</style>
