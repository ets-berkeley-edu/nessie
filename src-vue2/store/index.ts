import context from '@/store/modules/context'
import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

export default new Vuex.Store({
  modules: {context},
  strict: process.env.NODE_ENV !== 'production'
})
