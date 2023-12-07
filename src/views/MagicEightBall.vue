<template>
  <div v-if="chartOptions">
    <highcharts :options="chartOptions"></highcharts>
  </div>
</template>

<script>
import {Chart} from 'highcharts-vue'
import {get8BallSchedules} from '@/api/magicEightBall'

export default {
  components: {
    highcharts: Chart
  },
  data: () => ({
    chartOptions: null,
    colors: {
      red: '#b22222',
      green: '#33aa33',
      blue: '#6666ff',
      purple: '#bb66bb',
      paleRed: '#ffbbbb',
      paleGreen: '#aaeeaa',
      paleBlue: '#bbccff',
    }
  }),
  created() {
    get8BallSchedules().then(schedules => {
      let series = {
        design: [],
        development: [],
        qa: []
      }

      let scheduleMin = null
      let scheduleMax = null

      this.$_.each(schedules, s => {
        series.design.push({
          name: s.name,
          low: new Date(s.design).getTime(),
          high: new Date(s.development).getTime()
        })
        series.development.push({
          name: s.name,
          low: new Date(s.development).getTime(),
          high: new Date(s.qa).getTime()
        })
        series.qa.push({
          name: s.name,
          low: new Date(s.qa).getTime(),
          high: new Date(s.release).getTime()
        })
        if (!scheduleMin || scheduleMin > s.design) {
          scheduleMin = s.design
        }
        if (!scheduleMax || scheduleMax < s.release) {
          scheduleMax = s.release
        }
      })

      const colors = this.colors

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
        subtitle: {
          useHTML: true,
          text: '<br/><span style="font-size: 15px; color: ' + colors.red + '">⬤</span> Design<br/>' +
            '<span style="font-size: 15px; color: ' + colors.green + '">⬤</span> Development<br/>' +
            '<span style="font-size: 15px; color: ' + colors.blue + '">⬤</span> QA/bugfix<br/>' +
            '<span style="font-size: 15px; color: ' + colors.purple + '">⬤</span> Production release'
        },
        title: {
          text: '🎱 RTL DevOps Project Timeline'
        },
        tooltip: {
          shared: true,
          useHTML: true,
          formatter: function() {
            return '<b>' + this.points[0].point.name + '</b>' +
              (this.points[0].point.low === this.points[0].point.high ? '' : '<br/><span style="color: ' + colors.red + '">Design: ' + new Date(this.points[0].point.low + 100000000).toDateString() + ' </span>') +
              (this.points[0].point.high === this.points[1].point.high ? '' : '<br/><span style="color: ' + colors.green + '">Development: ' + new Date(this.points[0].point.high + 100000000).toDateString() + ' </span>') +
              '<br/><span style="color: ' + colors.blue + '">QA/bugfix : ' + new Date(this.points[1].point.high + 100000000).toDateString() + ' </span>' +
              '<br/><span style="color: ' + colors.purple + '">Production release: ' + new Date(this.points[2].point.high + 100000000).toDateString() + ' </span>'
          }
        },
        xAxis: {
          type: 'category'
        },
        yAxis: {
          type: 'datetime',
          min: new Date(scheduleMin).getTime(),
          max: new Date(scheduleMax).getTime(),
          title: {
            text: null
          }
        },
        plotOptions: {
          dumbbell: {
            grouping: false
          }
        },
        series: [
          {
            name: 'Design to development',
            data: series.design,
            connectorWidth: 15,
            color: colors.paleRed,
            lowColor: colors.red,
            marker: {
              fillColor: colors.green,
              symbol: 'circle',
              radius: 7
            }
          },
          {
            name: 'Development to qa',
            data: series.development,
            connectorWidth: 15,
            color: colors.paleGreen,
            lowColor: colors.green,
            marker: {
              fillColor: colors.blue,
              symbol: 'circle',
              radius: 7
            }
          },
          {
            name: 'QA to release',
            data: series.qa,
            connectorWidth: 15,
            lowColor: colors.blue,
            color: colors.paleBlue,
            marker: {
              fillColor: colors.purple,
              symbol: 'circle',
              radius: 7
            }
          }
        ]
      }
      this.$ready()
    })
  }
}
</script>
