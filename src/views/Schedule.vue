<template>
  <div>
    <h2>Schedule</h2>
    <div v-if="!contextStore.config.jobSchedulingEnabled" class="d-flex align-items-end mb-5 ml-3 pb-5 pt-3">
      <span class="pr-3">
        <v-icon :icon="mdiAlert" />
      </span>
      <span class="font-weight-bolder">Job scheduling is not enabled</span>
    </div>
    <div v-if="contextStore.config.jobSchedulingEnabled">
      <LargeSpinner v-if="contextStore.isLoading" />
      <v-expansion-panels v-if="!contextStore.isLoading">
        <div class="my-4 text-left w-100">
          <v-btn class="bg-blue-grey-darken-1" @click="refresh">Reload from configs</v-btn>
        </div>
        <v-expansion-panel
          v-for="job in jobs"
          :key="job.id"
        >
          <v-expansion-panel-title class="bg-blue-grey-lighten-5">
            {{ job.id }}
          </v-expansion-panel-title>
          <v-expansion-panel-text>
            <div>
              <b>Trigger:</b> {{ job.trigger }}
            </div>
            <div>
              <b>Next run:</b> {{ toRelativeTime(job.nextRun) }}
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
            <v-btn class="pa-2 mr-2 my-4 bg-blue-grey-darken-1" @click="pauseJob(job.id)">Pause</v-btn>
            <v-btn class="pa-2 my-4 bg-red-darken-4" @click="removeJob(job.id)">Remove</v-btn>
          </v-expansion-panel-text>
        </v-expansion-panel>
      </v-expansion-panels>
    </div>
  </div>
</template>

<script setup>
import {onMounted, ref} from 'vue'
import {capitalize} from 'lodash'
import {DateTime} from 'luxon'
import {mdiAlert} from '@mdi/js'

import LargeSpinner from '@/components/widgets/LargeSpinner'
import {getSchedule, reloadSchedule, removeSchedule, updateSchedule} from '@/api/schedule'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()

const jobs = ref([])

onMounted(() => {
  if (contextStore.config.jobSchedulingEnabled) {
    contextStore.loadingStart()
    getSchedule().then(data => {
      jobs.value = data
      contextStore.loadingComplete()
    })
  } else {
    contextStore.loadingComplete()
  }
})

const pauseJob = (jobId) => {
  contextStore.loadingStart()
  updateSchedule(jobId, {}).then(updatedJob => {
    jobs.value.forEach((job, index) => {
      if (job.id === updatedJob.id) {
        jobs.value.splice(index, 1)
      }
    })
    jobs.value.push(updatedJob)
    contextStore.loadingComplete()
  })
}

const refresh = () => {
  contextStore.loadingStart()
  reloadSchedule().then(data => {
    jobs.value = data
    contextStore.loadingComplete()
  })
}

const removeJob = (jobId) => {
  contextStore.loadingStart()
  removeSchedule(jobId).then(data => {
    jobs.value = data
    contextStore.loadingComplete()
  })
}

const toRelativeTime = (timestamp) => {
  if (!timestamp) {
    return null
  }
  const dt = DateTime.fromISO(timestamp.replace(' ', 'T'))
  return `${capitalize(dt.toRelativeCalendar())} at ${dt.toFormat('t')}`
}
</script>
