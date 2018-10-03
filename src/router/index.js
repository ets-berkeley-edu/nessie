import Vue from 'vue';
import Router from 'vue-router';
import store from '../store';

const ifNotAuthenticated = (to, from, next) => {
  if (!store.getters.isAuthenticated) {
    next();
    return;
  }
  next('/');
};

const ifAuthenticated = (to, from, next) => {
  if (store.getters.isAuthenticated) {
    next();
    return;
  }
  next('/login');
};

const routerOptions = [
  {
    path: '/',
    component: 'Home',
    beforeEnter: ifAuthenticated,
  },
  {
    path: '/account',
    component: 'Account',
    beforeEnter: ifAuthenticated,
  },
  {
    path: '/job/:id',
    component: 'Job',
    beforeEnter: ifAuthenticated,
  },
  {
    path: '/login',
    component: 'Login',
    beforeEnter: ifNotAuthenticated,
  },
  {
    path: '*',
    component: 'NotFound',
  },
];

const routes = routerOptions.map(route => ({
  ...route,
  component: () => import(`@/components/${route.component}.vue`),
  name: route.component,
}));

Vue.use(Router);

export default new Router({
  mode: 'history',
  routes,
});
