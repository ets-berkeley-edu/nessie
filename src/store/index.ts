import context from '@/store/modules/context';
import schedule from '@/store/modules/schedule';
import user from '@/store/modules/user';
import Vue from 'vue';
import Vuex from 'vuex';

Vue.use(Vuex);

export default new Vuex.Store({
  modules: {
    context,
    schedule,
    user
  },
  strict: process.env.NODE_ENV !== 'production'
});
