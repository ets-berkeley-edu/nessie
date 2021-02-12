<template>
  <div class="align-items-center d-flex justify-content-between mb-3">
    <div class="align-items-center d-flex">
      <div class="pr-2">
        <router-link :to="{name: 'home'}"><img src="@/assets/logo.png"></router-link>
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
    <div v-if="$currentUser" class="align-self-start pt-3">
      <b-link class="align-items-center d-flex greeting" @click="logOut">
        <span class="sr-only">Log Out</span>
        <b-icon font-scale="2" class="p-0" icon="box-arrow-right"></b-icon>
      </b-link>
    </div>
    <div v-if="!$currentUser">
      <form @submit.prevent="casLogin">
        <button id="cas-log-in" class="btn btn-default btn-primary splash-btn-sign-in">Sign In</button>
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
.breadcrumb span {
  padding: 5px;
}
.git-commit {
  font-size: 12px;
}
.greeting {
  color: #749461;
}
</style>
