import App from '@/App.vue';
import axios from 'axios';
import BootstrapVue from 'bootstrap-vue';
import router from '@/router';
import store from '@/store';
import Vue from 'vue';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap-vue/dist/bootstrap-vue.min.css';

// Allow cookies in Access-Control requests
axios.defaults.withCredentials = true;
axios.interceptors.response.use(
  response => response,
  error => Promise.reject(error)
);

Vue.use(BootstrapVue);

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app');
