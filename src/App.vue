<template>
  <div id="app">
    <Container>
      <Header/>
      <div class="content">
        <display-error/>
        <router-view/>
      </div>
      <Footer/>
    </Container>
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

<style src="./nessie.css">
</style>
