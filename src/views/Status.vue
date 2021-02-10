<template>
  <div>
    <h1>Nessie v{{ version.version }}</h1>
    <h2>Config</h2>
    <b-list-group class="w-50">
      <b-list-group-item
        v-for="(value, key) in $config"
        :key="key"
        class="d-flex justify-content-between align-items-center"
      >
        <b-badge pill variant="info"><span class="config-pill">{{ _.capitalize(decamelize(key)) }}</span></b-badge>
        {{ value }}
      </b-list-group-item>
    </b-list-group>
    <div class="py-3">
      <h2>Build</h2>
      <ul>
        <li>Artifact: {{ version.build && version.build.artifact ? version.build.artifact : '--' }}</li>
        <li>Git commit: {{ version.build && version.build.gitCommit ? version.build.gitCommit : '--' }}</li>
      </ul>
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
