import Vue from 'vue';
import Vuex from 'vuex';

Vue.use(Vuex);

const state = {
  apiBaseURL: process.env.VUE_APP_API_BASE_URL,
  errors: [],
  runnableJobs: [],
  user: null
};

const getters = {
  errors: state => {
    return state.errors;
  },
  runnableJobs: state => {
    return state.runnableJobs;
  },
  user: state => {
    return state.user;
  }
};

const mutations = {
  logout: state => {
    state.user = null;
  },
  registerMe: (state, user) => {
    state.user = user;
  },
  cacheRunnableJobs: (state, runnableJobs) => {
    state.runnableJobs = runnableJobs;
  },
  reportError: (state, error) => {
    error.id = new Date().getTime();
    state.errors.push(error);
  },
  dismissError: (state, id: number) => {
    const indexOf = state.errors.findIndex(e => e.id === id);
    if (indexOf > -1) {
      state.errors.splice(indexOf, 1);
    }
  }
};

export default new Vuex.Store({
  state,
  getters,
  mutations
});
