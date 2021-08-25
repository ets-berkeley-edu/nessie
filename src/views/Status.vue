<template>
  <div class="mx-3">
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
  </div>
</template>

<script>
export default {
  data: () => ({
    gitCommit: undefined
  }),
  created() {
    this.gitCommit = this.$_.get(this.version.build, 'gitCommit')
    this.$ready()
  },
  methods: {
    decamelize: s => s.replace(/([a-z\d])([A-Z])/g, '$1 $2').replace(/([A-Z]+)([A-Z][a-z\d]+)/g, '$1 $2')
  }
}
</script>
