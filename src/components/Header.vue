<template>
  <div class="header">
    <div class="logo">
      <router-link :to="{name: 'home'}"><img src="@/assets/logo.png"></router-link>
    </div>
    <div v-if="user" class="breadcrumb">
      <span><router-link to="/">Home</router-link></span>
      <span>|</span>
      <span><router-link to="schedule">Schedule</router-link></span>
      <span>|</span>
      <span><router-link to="status">Status</router-link></span>
    </div>
    <div v-if="user" class="flex-row greeting">
      <div>Hello {{ user.uid }}</div>
      <div>
        [<b-link @click="logOut()">Logout</b-link>]
      </div>
    </div>
  </div>
</template>

<script>
import { getCasLogoutURL } from '@/api/user';
import store from '@/store';

export default {
  name: 'Header',
  computed: {
    user() {
      return store.getters['user/user'];
    }
  },
  methods: {
    logOut() {
      getCasLogoutURL().then(data => {
        window.location.href = data.casLogoutURL;
      });
    }
  }
};
</script>

<style scoped lang="scss">
.breadcrumb span {
  padding: 5px;
}
.header {
  display: flex;
  justify-content: space-between;
}
.logo {
  padding-top: 10px;
}
.greeting {
  padding-top: 15px;
}
.greeting div {
  padding-left: 10px;
}
</style>
