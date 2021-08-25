import 'bootstrap-vue/dist/bootstrap-vue.min.css'
import 'bootstrap/dist/css/bootstrap.min.css'
import _ from 'lodash'
import App from '@/App.vue'
import axios from 'axios'
import lodash from 'lodash'
import router from '@/router'
import store from '@/store'
import Vue from 'vue'
import VueLodash from 'vue-lodash'
import {BootstrapVue, BootstrapVueIcons} from 'bootstrap-vue'

// Allow cookies in Access-Control requests
axios.defaults.withCredentials = true
axios.interceptors.response.use(response => response, function(error) {
  store.commit('context/reportError', {
    message: error.message,
    text: _.get(error.response, 'text'),
    status: _.get(error.response, 'status'),
    stack: error.stack
  })
  return Promise.reject(error)
})

Vue.config.productionTip = false
Vue.use(BootstrapVue)
Vue.use(BootstrapVueIcons)
Vue.use(require('vue-moment'))
Vue.use(VueLodash, {lodash})

Vue.prototype.$_ = _
Vue.prototype.$loading = () => store.dispatch('context/loadingStart')
Vue.prototype.$ready = () => store.dispatch('context/loadingComplete')

const apiBaseUrl = process.env.VUE_APP_API_BASE_URL

axios.get(`${apiBaseUrl}/api/config`).then(response => {
  Vue.prototype.$config = response.data

  axios.get(`${apiBaseUrl}/api/user/profile`).then(response => {
    Vue.prototype.$currentUser = response.data
    new Vue({
      router,
      store,
      render: h => h(App)
    }).$mount('#app')

    store.dispatch('context/init').then(_.noop)
  })
})
