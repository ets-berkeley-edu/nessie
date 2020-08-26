import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

const state = {
  runnableJobs: []
}

const getters = {
  runnableJobs: (state: any) => {
    return state.runnableJobs
  }
}

const mutations = {
  cacheRunnableJobs: (state: any, runnableJobs: any) => {
    state.runnableJobs = runnableJobs
  }
}

export default {
  namespaced: true,
  state,
  getters,
  mutations
}
