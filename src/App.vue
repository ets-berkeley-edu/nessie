<template>
  <b-container id="app" class="h-100 px-0" fluid>
    <b-row>
      <b-col class="mt-2 mx-4">
        <Header />
      </b-col>
    </b-row>
    <b-row>
      <b-col>
        <b-tabs v-if="$currentUser" v-model="tabIndex" align="center">
          <b-tab
            v-for="(path, title, index) in {Jobs: '/home', Schedule: '/schedule', Status: '/status'}"
            :key="title"
            :active="path === $route.path"
            :title="title"
            @click="go(index, path)"
          >
            <b-card-text>
              <LargeSpinner v-if="loading || isToggling" />
              <div v-show="!loading && !isToggling" class="mt-3 mx-4">
                <DisplayError />
                <router-view />
              </div>
            </b-card-text>
          </b-tab>
        </b-tabs>
      </b-col>
    </b-row>
    <b-row>
      <b-col>
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
      </b-col>
    </b-row>
    <b-row v-if="!isToggling && !loading" class="m-3 pb-3">
      <b-col>
        <img src="@/assets/uc-berkeley-logo.svg" />
      </b-col>
      <b-col class="text-right">
        &copy; 2021 The Regents of the University of California
      </b-col>
    </b-row>
  </b-container>
</template>

<script>
import Context from '@/mixins/Context'
import DisplayError from './components/DisplayError'
import Header from './components/Header'
import LargeSpinner from '@/components/widgets/LargeSpinner'

export default {
  name: 'App',
  mixins: [Context],
  components: {
    DisplayError,
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
