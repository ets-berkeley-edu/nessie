import Vue from "vue";
import App from "@/App.vue";
import router from "@/router";
import store from "@/store";
import Loading from "@/components/lib/loading.vue";
import Container from "@/components/lib/container.vue";
import axios from "axios";

Vue.prototype.$http = axios;

Vue.component("loading", Loading);
Vue.component("container", Container);

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount("#app");
