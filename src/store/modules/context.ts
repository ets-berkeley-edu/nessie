import Vue from 'vue'
import Vuex from 'vuex'
import {getVersion} from '@/api/status'

Vue.use(Vuex)

const state = {
  loading: false,
  version: undefined
}

const getters = {
  apiBaseUrl: (): any => process.env.VUE_APP_API_BASE_URL,
  loading: (state: any): boolean => state.loading,
  version: (state: any): any => state.version
}

const mutations = {
  loadingComplete: (state: any) => state.loading = false,
  loadingStart: (state: any) => state.loading = true,
  storeVersion: (state: any, version: any) => (state.version = version)
}

const actions = {
  init: ({commit}) => {
    return new Promise<void>(resolve => {
      getVersion().then(version => {
        commit('storeVersion', version)
        resolve()
      })
    })
  },
  loadingComplete: ({commit}) => commit('loadingComplete'),
  loadingStart: ({commit}) => commit('loadingStart')
}

export default {
  namespaced: true,
  state,
  getters,
  mutations,
  actions
}
