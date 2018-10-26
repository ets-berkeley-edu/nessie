import Vue from 'vue';
import Router from 'vue-router';
import store from './store';
import { getMyProfile, getCasLoginURL } from './api/user';
import Home from '@/views/Home.vue';
import Schedule from '@/views/Schedule.vue';

Vue.use(Router);

const router = new Router({
  mode: 'history',
  routes: [
    {
      path: '/',
      name: 'home',
      component: Home
    },
    {
      path: '/schedule',
      name: 'schedule',
      component: Schedule,
      meta: {
        requiresAuth: true
      }
    }
  ]
});

router.beforeEach((to, from, next) => {
  getMyProfile().then(me => {
    if (me) {
      store.commit('registerMe', me);
    }
    if (
      to.matched.some(record => record.meta.requiresAuth) &&
      !store.getters.user
    ) {
      getCasLoginURL().then(data => {
        window.location = data.casLoginURL;
      });
    } else {
      next();
    }
  });
});

export default router;
