import 'bootstrap-vue/dist/bootstrap-vue.min.css'
import 'bootstrap/dist/css/bootstrap.min.css'
import App from '@/App.vue'
import axios from 'axios'
import BootstrapVue from 'bootstrap-vue'
import lodash from 'lodash'
import router from '@/router'
import store from '@/store'
import Vue from 'vue'
import VueLodash from 'vue-lodash'

// Allow cookies in Access-Control requests
axios.defaults.withCredentials = true
axios.interceptors.response.use(response => response, function(error) {
  store.commit('context/reportError', {
    message: error.message,
    text: error.response.text,
    status: error.response.status,
    stack: error.stack
  })
  return Promise.reject(error)
})

Vue.config.productionTip = false
Vue.use(BootstrapVue)
Vue.use(require('vue-moment'))
Vue.use(VueLodash, { lodash })

axios.get(`${process.env.VUE_APP_API_BASE_URL}/api/config`).then(response => {
  Vue.prototype.$config = response.data

  new Vue({
    router,
    store,
    render: h => h(App)
  }).$mount('#app')
})
