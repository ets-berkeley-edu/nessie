<template>
  <v-container>
    <v-row>
      <v-col>
        <h2>🎱 RTL DevOps Project Timeline</h2>
      </v-col>
    </v-row>
    <v-row>
      <v-col class="timeline-dates">
        <div>
          <h3 v-if="selectedSchedule && !editing">{{ selectedSchedule.name }}</h3>
          <v-text-field
            v-if="creating"
            v-model="newSchedule.name"
            placeholder="Name"
            variant="solo"
            hide-details
          />
          <v-text-field
            v-if="selectedSchedule && editing"
            v-model="selectedSchedule.name"
            variant="solo"
            hide-details
          />
        </div>
        <div class="d-flex align-center my-1">
          <v-icon :icon="mdiCircle" :color="colors.red" />
          <div class="mx-2">Design</div>
          <strong v-if="selectedSchedule && !editing">{{ formatDate(selectedSchedule.design) }}</strong>
          <v-text-field
            v-if="creating"
            v-model="newSchedule.design"
            variant="solo"
            hide-details
          />
          <v-text-field
            v-if="selectedSchedule && editing"
            v-model="selectedSchedule.design"
            variant="solo"
            hide-details
          />
        </div>
        <div class="d-flex align-center my-1">
          <v-icon :icon="mdiCircle" :color="colors.green" />
          <div class="mx-2">Development</div>
          <strong v-if="selectedSchedule && !editing">{{ formatDate(selectedSchedule.development) }}</strong>
          <v-text-field
            v-if="creating"
            v-model="newSchedule.development"
            variant="solo"
            hide-details
          />
          <v-text-field
            v-if="selectedSchedule && editing"
            v-model="selectedSchedule.development"
            variant="solo"
            hide-details
          />
        </div>
        <div class="d-flex align-center my-1">
          <v-icon :icon="mdiCircle" :color="colors.blue" />
          <div class="mx-2">QA/bugfix</div>
          <strong v-if="selectedSchedule && !editing">{{ formatDate(selectedSchedule.qa) }}</strong>
          <v-text-field
            v-if="creating"
            v-model="newSchedule.qa"
            variant="solo"
            hide-details
          />
          <v-text-field
            v-if="selectedSchedule && editing"
            v-model="selectedSchedule.qa"
            variant="solo"
            hide-details
          />
        </div>
        <div class="d-flex align-center mt-1 mb-3">
          <v-icon :icon="mdiCircle" :color="colors.purple" />
          <div class="mx-2">Production release</div>
          <strong v-if="selectedSchedule && !editing">{{ formatDate(selectedSchedule.release) }}</strong>
          <v-text-field
            v-if="creating"
            v-model="newSchedule.release"
            variant="solo"
            hide-details
          />
          <v-text-field
            v-if="selectedSchedule && editing"
            v-model="selectedSchedule.release"
            variant="solo"
            hide-details
          />
        </div>
        <v-btn v-if="selectedSchedule && !editing" class="ma-2" @click="editing = true">Edit</v-btn>
        <v-btn v-if="creating" class="ma-2" @click="createSchedule">Save</v-btn>
        <v-btn v-if="creating" class="ma-2" @click="cancelCreateSchedule">Cancel</v-btn>
        <v-btn v-if="editing" class="mr-1" @click="updateSchedule">Save</v-btn>
        <v-btn v-if="editing" class="ma-1" @click="deleteSchedule">Delete</v-btn>
        <v-btn v-if="editing" class="ma-1" @click="cancelUpdateSchedule">Cancel</v-btn>
      </v-col>
      <v-col cols="6">
        <v-btn
          v-if="!creating && !editing"
          class="ml-3"
          color="blue-grey-darken-2"
          @click="startCreation"
        >
          New
        </v-btn>
        <v-checkbox
          id="hide-old-projects"
          v-model="hideOldProjects"
          label="Hide old projects"
          @change="refreshProjects"
        ></v-checkbox>
      </v-col>
    </v-row>
    <v-row v-if="chartOptions">
      <v-col cols="12">
        <highcharts :key="chartTimestamp" :options="chartOptions"></highcharts>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup>
import {clone, each} from 'lodash'
import {mdiCircle} from '@mdi/js'
import {onMounted, ref} from 'vue'

import {create8BallSchedule, delete8BallSchedule, get8BallSchedules, update8BallSchedule} from '@/api/magicEightBall'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()

const chartOptions = ref(null)
const chartTimestamp = ref(Date.now())
const colors = {
  red: '#b22222',
  green: '#33aa33',
  blue: '#6666ff',
  purple: '#bb66bb',
  paleRed: '#ffbbbb',
  paleGreen: '#aaeeaa',
  paleBlue: '#bbccff',
}
const creating = ref(false)
const editing = ref(false)
const hideOldProjects = ref(true)
const newSchedule = ref(null)
const schedules = ref([])
const selectedSchedule = ref(null)
const selectedScheduleIndex = ref(null)

onMounted(() => {
  get8BallSchedules().then(schedules => {
    setSchedules(schedules)
    renderTimeline()
  })
})

const cancelCreateSchedule = () => {
  creating.value = false
  newSchedule.value = {}
}

const cancelUpdateSchedule = () => {
  selectedSchedule.value = clone(schedules[selectedScheduleIndex])
  editing.value = false
}

const createSchedule = () => {
  create8BallSchedule(newSchedule.value).then(() => {
    get8BallSchedules().then(schedules => {
      newSchedule.value = {}
      setSchedules(schedules)
      renderTimeline()
      creating.value = false
    })
  })
}

const deleteSchedule = () => {
  delete8BallSchedule(selectedSchedule.value.id).then(() => {
    get8BallSchedules().then(schedules => {
      setSchedules(schedules)
      selectedSchedule.value = null
      selectedScheduleIndex.value = null
      renderTimeline()
      editing.value = false
    })
  })
}

const formatDate = (datestamp) => {
  return new Date(datestamp + 'T00:00-1200').toDateString()
}

const refreshProjects = () => {
  get8BallSchedules().then(newSchedules => {
    setSchedules(newSchedules)
    renderTimeline()
  })
}

const renderTimeline = () => {
  let series = {
    design: [],
    development: [],
    qa: []
  }

  let scheduleMin = null
  let scheduleMax = null

  each(schedules.value, s => {
    series.design.push({
      name: s.name,
      high: new Date(s.design).getTime(),
      low: new Date(s.development).getTime()
    })
    series.development.push({
      name: s.name,
      high: new Date(s.development).getTime(),
      low: new Date(s.qa).getTime()
    })
    series.qa.push({
      name: s.name,
      high: new Date(s.qa).getTime(),
      low: new Date(s.release).getTime(),
    })
    if (!scheduleMin || scheduleMin > s.design) {
      scheduleMin = s.design
    }
    if (!scheduleMax || scheduleMax < s.release) {
      scheduleMax = s.release
    }
  })

  chartOptions.value = {
    chart: {
      type: 'dumbbell',
      height: 50 * series.design.length,
      inverted: true,
      zoomType: 'y'
    },
    legend: {
      enabled: false
    },
    tooltip: {
      enabled: true,
      followPointer: false,
      pointFormatter: function() {
        const seriesNames = this.series.name.split(' to ')
        return (
          `<span style="color:'${this.series.color}">●</span> ${seriesNames[0]}: <b>${new Date(this.high).toUTCString().slice(0, -13)}</b><br/>` +
          `<span style="color:'${this.series.lowColor}">●</span> ${seriesNames[1]}: <b>${new Date(this.low).toUTCString().slice(0, -13)}</b>`)
      },
      positioner: function(labelWidth, labelHeight, point) {
        var tooltipX = Math.max(point.plotX, 0) + 500
        var tooltipY = point.plotY - 60
        return {
          x: tooltipX,
          y: tooltipY
        }
      }
    },
    xAxis: {
      type: 'category',
      labels: {
        events: {
          click: function() { selectSchedule(this.pos) }
        }
      }
    },
    yAxis: {
      type: 'datetime',
      min: new Date(scheduleMin).getTime(),
      max: new Date(scheduleMax).getTime(),
      title: {
        text: null
      },
      plotLines: [
        {
          color: '#aaa',
          label: {
            rotation: 0,
            style: {
              color: '#aaa'
            },
            text: new Date().toDateString()
          },
          width: 1,
          zIndex: 9999,
          value: new Date().getTime(),
        }
      ]
    },
    title: {
      text: null
    },
    plotOptions: {
      dumbbell: {
        findNearestPointBy: 'x',
        getExtremesFromAll: true,
        grouping: false
      }
    },
    series: [
      {
        name: 'Design to Development',
        data: series.design,
        connectorWidth: 15,
        color: colors.paleRed,
        lowColor: colors.green,
        marker: {
          fillColor: colors.red,
          symbol: 'circle',
          radius: 7
        }
      },
      {
        name: 'Development to QA',
        data: series.development,
        connectorWidth: 15,
        color: colors.paleGreen,
        lowColor: colors.blue,
        marker: {
          fillColor: colors.green,
          symbol: 'circle',
          radius: 7
        },
      },
      {
        name: 'QA to Release',
        data: series.qa,
        connectorWidth: 15,
        lowColor: colors.purple,
        color: colors.paleBlue,
        marker: {
          fillColor: colors.blue,
          symbol: 'circle',
          radius: 7
        }
      }
    ]
  }
  chartTimestamp.value = Date.now()
  contextStore.loadingComplete()
}

const selectSchedule = (index) => {
  selectedScheduleIndex.value = index
  selectedSchedule.value = clone(schedules.value[index])
}

const setSchedules = (newSchedules) => {
  if (hideOldProjects.value) {
    schedules.value = newSchedules.filter(s => (new Date(s.release)).getTime() > Date.now())
  } else {
    schedules.value = newSchedules
  }
}

const startCreation = () => {
  creating.value = true
  editing.value = false
  newSchedule.value = {}
  selectedScheduleIndex.value = null
  selectedSchedule.value = null
}

const updateSchedule = () => {
  update8BallSchedule(selectedSchedule.value.id, selectedSchedule.value).then(updatedSchedule => {
    schedules.value.forEach((schedule, index) => {
      if (schedule.id === updatedSchedule.id) {
        schedules.value.splice(index, 1, updatedSchedule)
        selectSchedule(index)
      }
    })
    renderTimeline()
    editing.value = false
  })
}
</script>

<style scoped>
h2 {
  font-size: 30px;
}
</style>

<style>
.highcharts-xaxis-labels text {
  cursor: pointer !important;
  font-size: 16px !important;
}

.highcharts-xaxis-labels text:hover {
  font-weight: bold;
}

.timeline-dates .v-field__input {
  min-height: 0 !important;
  padding-bottom: 5px !important;
  padding-top: 5px !important;
}
</style>
