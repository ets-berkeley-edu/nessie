<template>
  <div id="app">
    <Container>
      <Header />
      <div class="content">
        <display-error />
        <router-view />
      </div>
      <Footer />
    </Container>
  </div>
</template>

<script>
import Container from './components/Container';
import DisplayError from './components/DisplayError';
import Footer from './components/Footer';
import Header from './components/Header';

export default {
  name: 'App',
  components: {
    Container,
    DisplayError,
    Footer,
    Header
  }
};
</script>

<style src="./nessie.css">
</style>
