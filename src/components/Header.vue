<template>
  <div class="header">
    <div>
      <router-link
        class="brand"
        to="/">
        <img
          src="@/assets/logo.png"
          width="40px">
      </router-link>
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
.brand {
  display: flex;
  align-items: center;
}
.logout {
  &:hover {
    cursor: pointer;
  }
}
</style>
