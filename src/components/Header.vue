<template>
  <v-row :class="{'mb-3': !currentUser}">
    <v-col class="d-flex mx-4">
      <div class="mt-2 pr-2">
        <a :href="{name: 'home'}"><img :class="{'shimmy': shimmy}" src="@/assets/logo.png"></a>
      </div>
      <div class="ml-">
        <h1 class="mb-0 pb-0 pl-4">
          Nessie
          <span v-if="contextStore.version"> {{ contextStore.version.version }}</span>
        </h1>
        <div v-if="get(contextStore.version, 'build.gitCommit')" class="pl-4">
          <a :href="`https://github.com/ets-berkeley-edu/nessie/commit/${contextStore.version.build.gitCommit}`" target="_blank">
            <v-icon :icon="mdiGithub" color="black"></v-icon>
            <span class="git-commit greeting pl-2">{{ contextStore.version.build.gitCommit }}</span>
          </a>
        </div>
      </div>
    </v-col>
    <v-col v-if="currentUser" class="text-right mx-4">
      <a href="" @click="casLogout">
        <span class="sr-only">Log Out</span>
        <v-icon :icon="mdiArrowRightBox" class="primary"></v-icon>
      </a>
    </v-col>
    <v-col v-if="!currentUser" class="text-right mx-4">
      <form @submit.prevent="casLogin">
        <v-btn id="cas-log-in" class="btn btn-default btn-primary" type="submit">Sign In</v-btn>
      </form>
    </v-col>
  </v-row>
</template>

<script setup>
import {get} from 'lodash'
import {mdiArrowRightBox, mdiGithub} from '@mdi/js'
import {onMounted, ref} from 'vue'

import {getCasLoginURL, getCasLogoutURL} from '@/api/auth'
import {useContextStore} from '@/stores/context'

const contextStore = useContextStore()
const currentUser = contextStore.currentUser
const shimmy = ref(false)

onMounted(() => {
  contextStore.setEventHandler('homepage-refresh', () => {
    shimmy.value = true
    setTimeout(() => {shimmy.value = false}, 500)
  })
})

const casLogin = () => getCasLoginURL().then(data => window.location = data.casLoginURL)
const casLogout = () => getCasLogoutURL().then(data => window.location.href = data.casLogoutURL)
</script>

<style scoped>
@keyframes shake {
  0% { transform: translate(1px, 1px) rotate(0deg); }
  10% { transform: translate(-1px, -2px) rotate(-1deg); }
  20% { transform: translate(-3px, 0px) rotate(1deg); }
  30% { transform: translate(3px, 2px) rotate(0deg); }
  40% { transform: translate(1px, -1px) rotate(1deg); }
  50% { transform: translate(-1px, 2px) rotate(-1deg); }
  60% { transform: translate(-3px, 1px) rotate(0deg); }
  70% { transform: translate(3px, 1px) rotate(-1deg); }
  80% { transform: translate(-1px, -1px) rotate(1deg); }
  90% { transform: translate(1px, 2px) rotate(0deg); }
  100% { transform: translate(1px, -2px) rotate(-1deg); }
}
.git-commit {
  font-size: 12px;
}
.greeting {
  color: #749461;
}
.shimmy {
  animation: shake 0.5s;
  animation-iteration-count: infinite;
}
</style>
