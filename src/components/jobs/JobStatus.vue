<template>
  <div>
    <h2>Job Status</h2>
    <div>
      <Datepicker
        v-model="jobsDate"
        placeholder="Select Date"
        @disabled="loading"
        @closed="getBackgroundJobStatus"
      ></Datepicker>
    </div>
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
        <div v-if="!jobStatuses.rows.length">
          No jobs
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash'
import Datepicker from 'vuejs-datepicker'
import LargeSpinner from '@/components/widgets/LargeSpinner'
import { getBackgroundJobStatus } from '@/api/job'

export default {
  name: 'JobStatus',
  components: {
    Datepicker,
    LargeSpinner
  },
  data() {
    return {
      jobsDate: new Date(),
      jobStatuses: {
        rows: [],
        fields: [
          { key: 'id', sortable: true },
          { key: 'status', sortable: true },
          { key: 'details' },
          { key: 'started', sortable: true },
          { key: 'finished', sortable: true }
        ]
      },
      loading: true
    }
  },
  created() {
    this.getBackgroundJobStatus()
  },
  methods: {
    /* eslint no-undef: "warn" */
    getBackgroundJobStatus() {
      this.loading = true
      getBackgroundJobStatus(this.jobsDate).then(data => {
        this.jobStatuses.rows = _.map(data, row => {
          let style =
            row.status === 'failed'
              ? 'danger'
              : row.status === 'started'
                ? 'info'
                : 'success'
          row._cellVariants = { status: style }
          return row
        })
        this.loading = false
      })
    }
  }
}
</script>

<style scoped>
td {
  padding-left: 20px;
  vertical-align: top;
}
.results-container {
  padding: 30px 10px 0 10px;
}
</style>
