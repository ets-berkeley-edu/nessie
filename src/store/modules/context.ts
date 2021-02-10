import Vue from 'vue'
import Vuex from 'vuex'
import {getPing, getVersion} from '@/api/status'

Vue.use(Vuex)

const state = {
  errors: [],
  ping: undefined,
  version: undefined
}

const getters = {
  apiBaseUrl: (): any => process.env.VUE_APP_API_BASE_URL,
  errors: (state: any): any => state.errors,
  ping: (state: any): any => state.ping,
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
  reportError: (state: any, error: any) => {
    error.id = new Date().getTime()
    state.errors.push(error)
  },
  storePing: (state: any, ping: any) => (state.ping = ping),
  storeVersion: (state: any, version: any) => (state.version = version)
}

const actions = {
  clearErrors: ({ commit }) => commit('clearErrors'),
  init: ({ commit }) => {
    return new Promise<void>(resolve => {
      getPing().then(ping => {
        commit('storePing', ping)
        getVersion().then(version => {
          commit('storeVersion', version)
          resolve()
        })
      })
    })
  }
}

export default {
  namespaced: true,
  state,
  getters,
  mutations,
  actions
}
