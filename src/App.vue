<template>
  <div id="app">
    <Header/>
    <div class="main-container">
      <Container>
        <router-view/>
      </Container>
    </div>
    <Footer/>
  </div>
</template>

<script>
import Container from './components/Container.vue';
import Footer from './components/Footer.vue';
import Header from './components/Header.vue';

export default {
  name: 'App',
  components: {
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
