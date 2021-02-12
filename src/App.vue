<template>
  <div id="app">
    <Container>
      <Header />
      <b-tabs v-if="$currentUser" v-model="tabIndex" align="center">
        <b-tab
          v-for="(path, title, index) in {Jobs: '/home', Schedule: '/schedule', Status: '/status'}"
          :key="title"
          :active="path === $route.path"
          :title="title"
          @click="go(index, path)"
        >
          <b-card-text class="content">
            <LargeSpinner v-if="loading || isToggling" />
            <div v-show="!loading && !isToggling">
              <DisplayError />
              <router-view />
            </div>
          </b-card-text>
        </b-tab>
      </b-tabs>
      <b-carousel
        v-if="!$currentUser"
        fade
        indicators
        img-width="1024"
        img-height="480"
      >
        <b-carousel-slide img-src="https://images.snaptrip.com/wp-content/uploads/2019/09/25160842/Webp.net-resizeimage-2019-09-25T170259.153-1.jpg" />
        <b-carousel-slide img-src="https://images.snaptrip.com/wp-content/uploads/2019/09/25161806/lochness_1280p.jpg" />
        <b-carousel-slide img-src="https://images.snaptrip.com/wp-content/uploads/2019/09/26083649/Webp.net-resizeimage-2019-09-26T093602.246-1.jpg" />
      </b-carousel>
      <div v-if="!isToggling && !loading" class="pt-4">
        <Footer />
      </div>
    </Container>
  </div>
</template>

<script>
import Container from './components/Container'
import Context from '@/mixins/Context'
import DisplayError from './components/DisplayError'
import Footer from './components/Footer'
import Header from './components/Header'
import LargeSpinner from '@/components/widgets/LargeSpinner'

export default {
  name: 'App',
  mixins: [Context],
  components: {
    Container,
    DisplayError,
    Footer,
    Header,
    LargeSpinner
  },
  data: () => ({
    isToggling: false,
    tabIndex: undefined
  }),
  methods: {
    go(index, path) {
      if (index !== this.tabIndex) {
        this.isToggling = true
        this.$router.push({path}).then(() => {
          this.isToggling = false
        })
      }
    }
  }
}
</script>

<style src="./nessie.css">
</style>
