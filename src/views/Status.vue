<template>
  <div>
    <h2>Config</h2>
    <b-list-group class="w-50">
      <b-list-group-item
        v-for="key in $_.keys($config).sort()"
        :key="key"
        class="d-flex justify-content-between align-items-center"
      >
        <b-badge pill variant="info"><span class="config-pill">{{ $_.capitalize(decamelize(key)) }}</span></b-badge>
        {{ $config[key] }}
      </b-list-group-item>
    </b-list-group>
    <div class="py-3">
      <h2>Ping</h2>
      <ul>
        <li>App: {{ ping.app }}</li>
        <li>RDS: {{ ping.rds }}</li>
        <li>Redshift: {{ ping.redshift }}</li>
      </ul>
    </div>
  </div>
</template>

<script>
import Context from '@/mixins/Context'

export default {
  mixins: [Context],
  data: () => ({
    gitCommit: undefined
  }),
  created() {
    this.$loading()
    this.gitCommit = this.$_.get(this.version.build, 'gitCommit')
    this.$ready()
  },
  methods: {
    decamelize: s => s.replace(/([a-z\d])([A-Z])/g, '$1 $2').replace(/([A-Z]+)([A-Z][a-z\d]+)/g, '$1 $2')
  }
}
</script>

<style scoped>
.config-pill {
  font-size: 14px;
}
</style>
