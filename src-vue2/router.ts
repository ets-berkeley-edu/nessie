import Home from '@/views/Home.vue'
import MagicEightBall from '@/views/MagicEightBall.vue'
import Schedule from '@/views/Schedule.vue'
import Status from '@/views/Status.vue'
import store from '@/store'
import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)

const router = new VueRouter({
  mode: 'history',
  routes: [
    {
      path: '/',
      redirect: '/home'
    },
    {
      path: '/8ball',
      component: MagicEightBall
    },
    {
      path: '/home',
      name: 'home',
      component: Home
    },
    {
      path: '/schedule',
      component: Schedule
    },
    {
      path: '/status',
      component: Status
    },
    {
      path: '*',
      redirect: '/home'
    }
  ]
})

router.beforeEach((to: any, from: any, next: Function) => {
  store.commit('context/loadingStart')
  next()
})

export default router
