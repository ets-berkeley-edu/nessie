/* eslint-disable promise/param-names */
import { AUTH_REQUEST, AUTH_ERROR, AUTH_SUCCESS, AUTH_LOGOUT } from '@/store/actions/auth';
import { USER_REQUEST } from '@/store/actions/user';
import authApi from '@/services/api/auth';

const state = { token: localStorage.getItem('user-token') || '', status: '', hasLoadedOnce: false };

const getters = {
  isAuthenticated: s => !!s.token,
  authStatus: s => s.status,
};

const actions = {
  [AUTH_REQUEST]: ({ commit, dispatch }, user) => new Promise((resolve, reject) => {
    commit(AUTH_REQUEST);
    authApi({ url: 'auth', data: user, method: 'POST' })
      .then((resp) => {
        localStorage.setItem('user-token', resp.token);
        // Here set the header of your ajax library to the token value.
        // example with axios
        // axios.defaults.headers.common['Authorization'] = resp.token
        commit(AUTH_SUCCESS, resp);
        dispatch(USER_REQUEST);
        resolve(resp);
      })
      .catch((err) => {
        commit(AUTH_ERROR, err);
        localStorage.removeItem('user-token');
        reject(err);
      });
  }),
  [AUTH_LOGOUT]: ({ commit }) => new Promise((resolve) => {
    commit(AUTH_LOGOUT);
    localStorage.removeItem('user-token');
    resolve();
  }),
};

const mutations = {
  [AUTH_REQUEST]: (s) => {
    s.status = 'loading';
  },
  [AUTH_SUCCESS]: (s, resp) => {
    s.status = 'success';
    s.token = resp.token;
    s.hasLoadedOnce = true;
  },
  [AUTH_ERROR]: (s) => {
    s.status = 'error';
    s.hasLoadedOnce = true;
  },
  [AUTH_LOGOUT]: (s) => {
    s.token = '';
  },
};

export default {
  state,
  getters,
  actions,
  mutations,
};
