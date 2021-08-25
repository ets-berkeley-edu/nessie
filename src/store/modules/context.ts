import Vue from 'vue'
import Vuex from 'vuex'
import {getVersion} from '@/api/status'

Vue.use(Vuex)

const state = {
  errors: [],
  loading: false,
  version: undefined
}

const getters = {
  apiBaseUrl: (): any => process.env.VUE_APP_API_BASE_URL,
  errors: (state: any): any => state.errors,
  loading: (state: any): boolean => state.loading,
  version: (state: any): any => state.version
}

const mutations = {
  clearErrors: (state: any) => (state.errors = []),
  dismissError: (state: any, id: number) => {
    const indexOf = state.errors.findIndex((e: any) => e.id === id)
    if (indexOf > -1) {
      state.errors.splice(indexOf, 1)
    }
  },
  loadingComplete: (state: any) => state.loading = false,
  loadingStart: (state: any) => state.loading = true,
  reportError: (state: any, error: any) => {
    error.id = new Date().getTime()
    state.errors.push(error)
  },
  storeVersion: (state: any, version: any) => (state.version = version)
}

const actions = {
  clearErrors: ({commit}) => {
    return new Promise<void>(resolve => {
      commit('clearErrors')
      resolve()
    })
  },
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
