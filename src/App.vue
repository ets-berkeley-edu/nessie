<template>
  <v-container id="app" class="h-100 px-0" fluid>
    <Header />
    <v-row v-if="currentUser">
      <v-tabs v-model="tabIndex" align-tabs="center" class="w-100">
        <v-tab
          v-for="(path, title, index) in {Jobs: '/', Schedule: '/schedule', Configs: '/configs', MagicEightBall: '/8ball'}"
          :key="title"
          @click="go(index, path)"
        >
          {{ title === 'MagicEightBall' ? '🎱' : title }}
        </v-tab>
      </v-tabs>
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
        <img src="@/assets/uc-berkeley-logo.svg" />
      </v-col>
      <v-col class="text-right">
        &copy; 2025 The Regents of the University of California
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup>
import {ref} from 'vue'

import Header from '@/components/Header.vue'
import LargeSpinner from '@/components/widgets/LargeSpinner.vue'
import LochNess from '@/components/LochNess.vue'
import router from '@/router'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()
const currentUser = contextStore.currentUser

const isToggling = ref(false)
const tabIndex = ref(undefined)

const go = (index, path) => {
  isToggling.value = true
  router.push({path: path}).then(() => {
    isToggling.value = false
  })
}
</script>
