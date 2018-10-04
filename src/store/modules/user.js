import Vue from 'vue';
import { USER_REQUEST, USER_ERROR, USER_SUCCESS } from '@/store/actions/user';
import authApi from '@/api/auth';
import { AUTH_LOGOUT } from '@/store/actions/auth';

const state = { status: '', profile: {} };

const getters = {
  getProfile: s => s.profile,
  isProfileLoaded: s => !!s.profile.name,
};

const actions = {
  [USER_REQUEST]: ({ commit, dispatch }) => {
    commit(USER_REQUEST);
    authApi({ url: 'user/me' })
      .then((resp) => {
        commit(USER_SUCCESS, resp);
      })
      .catch(() => {
        commit(USER_ERROR);
        // if resp is unauthorized, logout, to
        dispatch(AUTH_LOGOUT);
      });
  },
};

const mutations = {
  [USER_REQUEST]: () => {
    state.status = 'loading';
  },
  [USER_SUCCESS]: (s, resp) => {
    s.status = 'success';
    Vue.set(state, 'profile', resp);
  },
  [USER_ERROR]: () => {
    state.status = 'error';
  },
  [AUTH_LOGOUT]: () => {
    state.profile = {};
  },
};

export default {
  state,
  getters,
  actions,
  mutations,
};
