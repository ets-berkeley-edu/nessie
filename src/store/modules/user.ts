import Vue from 'vue';
import Vuex from 'vuex';

Vue.use(Vuex);

const state = {
  user: null
};

const getters = {
  user: (state: any) => {
    return state.user;
  }
};

const mutations = {
  logout: (state: any) => {
    state.user = null;
  },
  registerMe: (state: any, user: any) => {
    state.user = user;
  }
};

export default {
  namespaced: true,
  state,
  getters,
  mutations
};
