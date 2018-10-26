<template>
  <div id="app">
    <div class="main-container">
      <Container>
        <Header/>
        <display-error/>
        <router-view/>
        <Footer/>
      </Container>
    </div>
  </div>
</template>

<script>
import Container from './components/Container.vue';
import DisplayError from './components/DisplayError.vue';
import Footer from './components/Footer.vue';
import Header from './components/Header.vue';

export default {
  name: 'App',
  components: {
    DisplayError,
    Header,
    Container,
    Footer
  }
};
</script>

<style scoped lang="scss">
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
}
</style>
