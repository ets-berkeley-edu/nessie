<template>
  <div id="app">
    <navigation/>
    <div class="main-container">
      <container>
        <router-view/>
      </container>
    </div>
    <footer/>
  </div>
</template>

<script>
import Navigation from './components/Navigation.vue';
import { USER_REQUEST } from './store/actions/user';
import Footer from './components/Footer.vue';

export default {
  name: 'app',
  components: {
    Navigation,
    Footer,
  },
  created() {
    if (this.$store.getters.isAuthenticated) {
      this.$store.dispatch(USER_REQUEST);
    }
  },
};
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
