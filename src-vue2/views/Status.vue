<template>
  <div class="mx-3 pt-3">
    <b-table
      :fields="fields"
      head-row-variant="info"
      :items="items"
      responsive
      striped
    />
  </div>
</template>

<script>
export default {
  data: () => ({
    fields: [
      {key: 'key', label: 'Config', sortable: true},
      {key: 'value', label: 'Value'}
    ],
    items: undefined
  }),
  created() {
    this.items = []
    this.$_.each(this.$_.keys(this.$config).sort(), key => {
      const formattedKey = key.replace(/([a-z\d])([A-Z])/g, '$1_$2').replace(/([A-Z]+)([A-Z][a-z\d]+)/g, '$1_$2').toUpperCase()
      this.items.push({
        key: formattedKey,
        value: this.$config[key]
      })
    })
    this.$ready()
  }
}
</script>
