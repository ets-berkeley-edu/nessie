<template>
  <v-container id="app" class="h-100 px-0" fluid>
    <Header />
    <v-row v-if="currentUser">
      <div class="border-b-md border-color-success w-100">
        <v-tabs
          v-model="tabIndex"
          align-tabs="center"
          class="w-100"
          align="center"
          density="comfortable"
        >
          <div
            v-for="item in selectablePaths"
            :key="item.title"
            class="pr-2"
          >
            <v-tab
              class="border-s-sm border-e-sm border-t-sm rounded-t-lg"
              :class="{
                'bg-white border-b-0 font-weight-bold text-success': item.path === tabIndex,
                'bg-grey-lighten-4 border-b-md': item.path !== tabIndex
              }"
              :value="item.path"
              @click="go(item.path)"
            >
              {{ item.title }}
            </v-tab>
          </div>
        </v-tabs>
      </div>
      <v-card-text>
        <LargeSpinner v-if="contextStore.isLoading || isToggling" />
        <div v-show="!contextStore.isLoading && !isToggling" class="mt-3 mx-4">
          <router-view />
        </div>
      </v-card-text>
    </v-row>
    <v-row v-if="!currentUser">
      <LochNess />
    </v-row>
    <v-row v-if="!isToggling && !contextStore.isLoading" class="mx-4 pb-3">
      <v-col>
        <img alt="UC Berkeley logo" src="@/assets/uc-berkeley-logo.svg">
      </v-col>
      <v-col class="text-right">
        &copy; 2025 The Regents of the University of California
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup>
import {ref, watch} from 'vue'

import Header from '@/components/Header.vue'
import LargeSpinner from '@/components/widgets/LargeSpinner.vue'
import LochNess from '@/components/LochNess.vue'
import router from '@/router'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()
const currentUser = contextStore.currentUser

const isToggling = ref(false)
const tabIndex = ref(undefined)

const selectablePaths = [
  {title: 'Jobs', path: '/'},
  {title: 'Schedule', path: '/schedule'},
  {title: 'configs', path: '/configs'},
  {title: '🎱', path: '/8ball'}
]

const go = (path) => {
  isToggling.value = true
  router.push({path: path}).then(() => {
    isToggling.value = false
    tabIndex.value = path
  })
}

watch(router.currentRoute, route => { tabIndex.value = route.path})
</script>

<style scoped>
.border-color-success {
  border-color: rgb(var(--v-theme-success)) !important;
}
</style>
