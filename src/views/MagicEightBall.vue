<template>
  <div>
    <b-row>
      <b-col>
        <h2>🎱 RTL DevOps Project Timeline</h2>
      </b-col>
    </b-row>
    <b-row>
      <b-col>
        <div>
          <strong v-if="selectedSchedule && !editing">{{ selectedSchedule.name }}</strong>
          <input v-if="creating" v-model="newSchedule.name" class="w-100" />
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.name" class="w-100" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.red">⬤</span> Design
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.design) }}</strong></span>
          <input v-if="creating" v-model="newSchedule.design" />
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.design" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.green">⬤</span> Development
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.development) }}</strong></span>
          <input v-if="creating" v-model="newSchedule.development" />
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.development" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.blue">⬤</span> QA/bugfix
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.qa) }}</strong></span>
          <input v-if="creating" v-model="newSchedule.qa" />
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.qa" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.purple">⬤</span> Production release
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.release) }}</strong></span>
          <input v-if="creating" v-model="newSchedule.release" />
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.release" />
        </div>
        <b-btn v-if="selectedSchedule && !editing" @click="editing = true">Edit</b-btn>
        <b-btn v-if="creating" @click="createSchedule">Save</b-btn>
        <b-btn v-if="creating" @click="cancelCreateSchedule">Cancel</b-btn>
        <b-btn v-if="editing" @click="updateSchedule">Save</b-btn>
        <b-btn v-if="editing" @click="deleteSchedule">Delete</b-btn>
        <b-btn v-if="editing" @click="cancelUpdateSchedule">Cancel</b-btn>
      </b-col>
      <b-col cols="6">
        <b-btn v-if="!creating && !editing" class="mb-2" @click="startCreation">
          New
        </b-btn>
        <b-form-checkbox id="hide-old-projects" v-model="hideOldProjects" @change="refreshProjects">
          Hide old projects
        </b-form-checkbox>
      </b-col>
    </b-row>
    <div v-if="chartOptions">
      <highcharts :key="chartTimestamp" :options="chartOptions"></highcharts>
    </div>
  </div>
</template>

<script>
import {Chart} from 'highcharts-vue'
import {create8BallSchedule, delete8BallSchedule, get8BallSchedules, update8BallSchedule} from '@/api/magicEightBall'

export default {
  components: {
    highcharts: Chart
  },
  data: () => ({
    chartOptions: null,
    chartTimestamp: Date.now(),
    colors: {
      red: '#b22222',
      green: '#33aa33',
      blue: '#6666ff',
      purple: '#bb66bb',
      paleRed: '#ffbbbb',
      paleGreen: '#aaeeaa',
      paleBlue: '#bbccff',
    },
    creating: false,
    editing: false,
    hideOldProjects: true,
    newSchedule: null,
    schedules: [],
    selectedSchedule: null,
    selectedScheduleIndex: null
  }),
  created() {
    get8BallSchedules().then(schedules => {
      this.setSchedules(schedules)
      this.renderTimeline()
    })
  },
  methods: {
    cancelCreateSchedule() {
      this.creating = false
      this.newSchedule = {}
    },
    cancelUpdateSchedule() {
      this.selectedSchedule = this.$_.clone(this.schedules[this.selectedScheduleIndex])
      this.editing = false
    },
    createSchedule() {
      create8BallSchedule(this.newSchedule).then(() => {
        get8BallSchedules().then(schedules => {
          this.newSchedule = {}
          this.setSchedules(schedules)
          this.renderTimeline()
          this.creating = false
        })
      })
    },
    deleteSchedule() {
      delete8BallSchedule(this.selectedSchedule.id).then(() => {
        get8BallSchedules().then(schedules => {
          this.setSchedules(schedules)
          this.selectedSchedule = null
          this.selectedScheduleIndex = null
          this.renderTimeline()
          this.editing = false
        })
      })
    },
    formatDate(datestamp) {
      return new Date(datestamp + 'T00:00-1200').toDateString()
    },
    refreshProjects() {
      get8BallSchedules().then(schedules => {
        this.setSchedules(schedules)
        this.renderTimeline()
      })
    },
    renderTimeline() {
      let series = {
        design: [],
        development: [],
        qa: []
      }

      let scheduleMin = null
      let scheduleMax = null

      this.$_.each(this.schedules, s => {
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

      const colors = this.colors
      const selectSchedule = this.selectSchedule

      this.chartOptions = {
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
      this.chartTimestamp = Date.now()
      this.$ready()
    },
    selectSchedule(index) {
      this.selectedScheduleIndex = index
      this.selectedSchedule = this.$_.clone(this.schedules[index])
    },
    setSchedules(schedules) {
      if (this.hideOldProjects) {
        this.schedules = schedules.filter(s => (new Date(s.release)).getTime() > Date.now())
      } else {
        this.schedules = schedules
      }
    },
    startCreation() {
      this.creating = true
      this.editing = false
      this.newSchedule = {}
      this.selectedScheduleIndex = null
      this.selectedSchedule = null
    },
    updateSchedule() {
      update8BallSchedule(this.selectedSchedule.id, this.selectedSchedule).then(updatedSchedule => {
        this.schedules.forEach((schedule, index) => {
          if (schedule.id === updatedSchedule.id) {
            this.schedules.splice(index, 1, updatedSchedule)
            this.selectSchedule(index)
          }
        })
        this.renderTimeline()
        this.editing = false
      })
    }
  }
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
</style>
