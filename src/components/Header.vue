<template>
  <div class="header">
    <div>
      <router-link to="/"><img src="@/assets/logo.png" width="40px"></router-link>
    </div>
    <div class="breadcrumb">
      <router-link to="/">Home</router-link>
      | <router-link to="schedule" v-if="user">Schedule</router-link>
    </div>
    <div v-if="user">
      <div>Hello {{ user.uid }}</div>
      <button class="logout" @click="logOut">
        Logout
      </button>
    </div>
    <div v-if="!user">
      <button @click="logIn">Log in</button>
    </div>
  </div>
</template>

<script>
import { getCasLoginURL, getCasLogoutURL } from '@/api/user';
import store from '@/store';

export default {
  name: 'Header',
  computed: {
    user() {
      return store.getters.user;
    }
  },
  methods: {
    logOut() {
      getCasLogoutURL().then(data => {
        window.location.href = data.casLogoutURL;
      });
    },
    logIn() {
      getCasLoginURL().then(data => {
        window.location.href = data.casLoginURL;
      });
    }
  }
};
</script>

<style scoped lang="scss">
a {
  text-decoration: none;
}
.header {
  display: flex;
  justify-content: space-between;
  padding: 25px;
}
.breadcrumb {
  display: flex;
  align-items: center;
}
.breadcrumb a {
  padding: 0 10px 0 10px;
}
.logout {
  &:hover {
    cursor: pointer;
  }
}
</style>
