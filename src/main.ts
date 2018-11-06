import 'bootstrap-vue/dist/bootstrap-vue.min.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import App from '@/App.vue';
import Vue from 'vue';
import VueLodash from 'vue-lodash';
import axios from 'axios';
import BootstrapVue from 'bootstrap-vue';
import router from '@/router';
import store from '@/store';

// Allow cookies in Access-Control requests
axios.defaults.withCredentials = true;
axios.interceptors.response.use(response => response, function(error) {
  store.commit('reportError', error);
  return Promise.reject(error);
});

Vue.config.productionTip = false;
Vue.use(BootstrapVue);
Vue.use(require('vue-moment'));
Vue.use(VueLodash);

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app');
