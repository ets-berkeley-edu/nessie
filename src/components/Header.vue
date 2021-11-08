<template>
  <div class="align-items-center d-flex justify-content-between" :class="{'mb-3': !$currentUser}">
    <div class="align-items-center d-flex">
      <div class="mt-2 pr-2">
        <router-link :to="{name: 'home'}"><img :class="{'shimmy': shimmy}" src="@/assets/logo.png"></router-link>
      </div>
      <div>
        <h1 class="mb-0 pb-0">Nessie<span v-if="version"> v{{ version.version }}</span></h1>
        <div v-if="_.get(version, 'build.gitCommit')">
          <b-link :href="`https://github.com/ets-berkeley-edu/nessie/commit/${version.build.gitCommit}`" target="_blank">
            <span class="git-commit greeting pr-1">{{ version.build.gitCommit }}</span>
            <b-icon icon="github" variant="dark"></b-icon>
          </b-link>
        </div>
      </div>
    </div>
    <div v-if="$currentUser">
      <b-link class="align-items-center d-flex greeting" @click="logOut">
        <span class="sr-only">Log Out</span>
        <b-icon font-scale="2" icon="box-arrow-right"></b-icon>
      </b-link>
    </div>
    <div v-if="!$currentUser" class="pt-2">
      <form @submit.prevent="casLogin">
        <button id="cas-log-in" class="btn btn-default btn-primary">Sign In</button>
      </form>
    </div>
  </div>
</template>

<script>
import Context from '@/mixins/Context'
import {getCasLoginURL, getCasLogoutURL} from '@/api/user'

export default {
  name: 'Header',
  mixins: [Context],
  data: () => ({
    shimmy: false
  }),
  created() {
    this.$eventHub.on('homepage-refresh', () => {
      this.shimmy = true
      setTimeout(() => {this.shimmy = false}, 500)
    })
  },
  methods: {
    casLogin() {
      getCasLoginURL().then(data => window.location = data.casLoginURL)
    },
    logOut() {
      getCasLogoutURL().then(data => window.location.href = data.casLogoutURL)
    }
  }
}
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
