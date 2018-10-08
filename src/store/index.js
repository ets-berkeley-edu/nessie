import Vue from "vue";
import Vuex from "vuex";

Vue.use(Vuex);

export default new Vuex.Store({
  state: {
    apiBaseURL: process.env.VUE_APP_API_BASE_URL,
    user: null
  },
  getters: {
    user(state) {
      return state.user;
    }
  },
  mutations: {
    registerMe(state, user) {
      state.user = user;
    },
    logout(state) {
      state.user = null;
    }
  }
});
