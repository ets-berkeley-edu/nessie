import Vue from 'vue';
import VueRouter, { Route } from 'vue-router';
import store from '@/store';
import { getRunnableJobs } from '@/api/job';
import { getMyProfile } from '@/api/user';
import Login from '@/views/Login.vue';
import Home from '@/views/Home.vue';
import Schedule from '@/views/Schedule.vue';

Vue.use(VueRouter);

let registerMe = () => {
  return Promise.resolve(
    getMyProfile().then(me => {
      if (me) {
        store.commit('registerMe', me);
        getRunnableJobs().then(data => {
          store.commit('cacheRunnableJobs', data);
          return me;
        });
      } else {
        return null;
      }
    })
  );
};

let beforeEach = (to: Route, from: Route, next: Function) => {
  let safeNext = (to: Route, next: Function) => {
    if (to.matched.length) {
      next();
    } else {
      next('/home');
    }
  };
  if (store.getters.user) {
    safeNext(to, next);
  } else {
    registerMe().then(() => safeNext(to, next));
  }
};

let requiresAuth = (to: Route, from: Route, next: Function) => {
  if (store.getters.user) {
    next();
  } else {
    next('/login');
  }
};

const router = new VueRouter({
  mode: 'history',
  routes: [
    {
      path: '/login',
      name: 'login',
      component: Login,
      beforeEnter: (to: Route, from: Route, next: Function) => {
        if (store.getters.user) {
          next('/home');
        } else {
          next();
        }
      }
    },
    {
      path: '/home',
      name: 'home',
      component: Home,
      beforeEnter: requiresAuth
    },
    {
      path: '/schedule',
      component: Schedule,
      beforeEnter: requiresAuth
    }
  ]
});

router.beforeEach(beforeEach);

export default router;
