<template>
  <div class="navigation">
    <ul>
      <li>
        <router-link
          class="brand"
          to="/">
          <img
            src="@/assets/logo.png"
            width="40px"><strong>Nessie</strong>
        </router-link>
      </li>
    </ul>
    <ul>
      <li
        v-if="user"
        @click="logOut">
        <span class="logout">Logout</span>
      </li>
      <li v-if="!user">
        <button @click="logIn">Log in</button>
      </li>
    </ul>
  </div>
</template>

<style lang="scss" scoped>
a {
  color: white;
  text-decoration: none;
}
.navigation {
  display: flex;
  color: white;
  align-items: center;
  background-color: #ffa035;
  padding: 5px;
  ul {
    display: flex;
    &:first-child {
      flex-grow: 1;
    }
    li {
      padding-right: 1em;
    }
  }
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

<script>
import UserApi from "@/services/api/UserApi.js";
import store from "@/store";

export default {
  name: "Navigation",
  computed: {
    user() {
      return store.getters.user;
    }
  },
  methods: {
    logOut() {
      this.$store.dispatch("logout");
    },
    logIn() {
      UserApi.getCasLoginURL().then(url => {
        window.location = url;
      });
    }
  }
};
</script>
