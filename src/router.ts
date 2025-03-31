const LochNess = () => import('./views/LochNess.vue')

import {createRouter, createWebHistory, RouteRecordRaw} from 'vue-router'

const routes:RouteRecordRaw[] = [
  {
    component: LochNess,
    path: '/',
  }
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
})

export default router
