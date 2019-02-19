import Home from '@/views/Home.vue';
import Login from '@/views/Login.vue';
import Schedule from '@/views/Schedule.vue';
import Status from '@/views/Status.vue';
import store from '@/store';
import Vue from 'vue';
import VueRouter from 'vue-router';
import { getMyProfile } from '@/api/user';
import { getRunnableJobs } from '@/api/job';

Vue.use(VueRouter);

let registerMe = () => {
  return Promise.resolve(
    getMyProfile().then(me => {
      if (me) {
        store.commit('user/registerMe', me);
        getRunnableJobs().then(data => {
          store.commit('schedule/cacheRunnableJobs', data);
          return me;
        });
      } else {
        return null;
      }
    })
  );
};

let beforeEach = (to: any, from: any, next: Function) => {
  store.dispatch('context/loadConfig').then(() => {
    store.dispatch('context/clearErrors').then(() => {
      let safeNext = (to: any, next: Function) => {
        if (to.matched.length) {
          next();
        } else {
          next('/home');
        }
      };
      if (store.getters['user/user']) {
        safeNext(to, next);
      } else {
        registerMe().then(() => safeNext(to, next));
      }
    });
  });
};

let requiresAuth = (to: any, from: any, next: Function) => {
  if (store.getters['user/user']) {
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
      beforeEnter: (to: any, from: any, next: Function) => {
        if (store.getters['user/user']) {
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
    },
    {
      path: '/status',
      component: Status,
      beforeEnter: requiresAuth
    }
  ]
});

router.beforeEach(beforeEach);

export default router;
