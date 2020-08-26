import Vue from 'vue'
import Vuex from 'vuex'
import _ from 'lodash'
import { getConfig, getPing, getVersion } from '@/api/status'

Vue.use(Vuex)

const state = {
  config: undefined,
  errors: [],
  ping: undefined,
  version: undefined
}

const getters = {
  apiBaseUrl: (): any => process.env.VUE_APP_API_BASE_URL,
  currentEnrollmentTermId: (state: any): boolean =>
    _.get(state.config, 'currentEnrollmentTermId'),
  currentEnrollmentTerm: (state: any): boolean =>
    _.get(state.config, 'currentEnrollmentTerm'),
  ebEnvironment: (state: any): string => _.get(state.config, 'ebEnvironment'),
  nessieEnv: (state: any): string => _.get(state.config, 'nessieEnv'),
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
  storeConfig: (state: any, config: any) => (state.config = config),
  storePing: (state: any, ping: any) => (state.ping = ping),
  storeVersion: (state: any, version: any) => (state.version = version)
}

const actions = {
  clearErrors: ({ commit }) => commit('clearErrors'),
  loadConfig: ({ commit, state }) => {
    return new Promise(resolve => {
      if (state.config) {
        resolve(state.config)
      } else {
        getPing().then(ping => {
          commit('storePing', ping)
          getVersion().then(version => {
            commit('storeVersion', version)
            getConfig().then(config => {
              commit('storeConfig', config)
              resolve(config)
            })
          })
        })
      }
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
