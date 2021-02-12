import Home from '@/views/Home.vue'
import Schedule from '@/views/Schedule.vue'
import Status from '@/views/Status.vue'
import store from '@/store'
import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)

const beforeEach = (to: any, from: any, next: Function) => {
  store.dispatch('context/init').then(() => {
    store.dispatch('context/clearErrors').then(() => {
      const safeNext = (to: any, next: Function) => {
        if (to.matched.length) {
          next()
        } else {
          next('/home')
        }
      }
      safeNext(to, next)
    })
  })
}

const requiresAuth = (to: any, from: any, next: Function) => {
  if (Vue.prototype.$currentUser) {
    next()
  } else {
    next('/login')
  }
}

const router = new VueRouter({
  mode: 'history',
  routes: [
    {
      path: '/',
      redirect: '/home'
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
})

router.beforeEach(beforeEach)

export default router
