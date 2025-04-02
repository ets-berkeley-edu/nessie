<template>
  <div class="mx-3 striped-table">
    <v-data-table
      :headers="headers"
      :items="items"
      items-per-page="-1"
      hide-default-footer
    />
  </div>
</template>

<script setup>
import {keys, map} from 'lodash'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()

const headers = [
  {key: 'key', title: 'Config'},
  {key: 'value', title: 'Value'}
]

const items = map(keys(contextStore.config).sort(), key => {
  const formattedKey = key.replace(/([a-z\d])([A-Z])/g, '$1_$2').replace(/([A-Z]+)([A-Z][a-z\d]+)/g, '$1_$2').toUpperCase()
  return {
    key: formattedKey,
    value: contextStore.config[key]
  }
})
</script>

<style>
.striped-table .v-data-table th {
  font-weight: bold;
}
.striped-table .v-data-table tbody tr:nth-of-type(even) {
    background-color: rgba(0, 0, 0, .05);
}
</style>
