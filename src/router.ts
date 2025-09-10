import type {RouteRecordRaw} from 'vue-router'
import {createRouter, createWebHistory} from 'vue-router'

const Configs = () => import('./views/Configs.vue')
const JobTable = () => import('./views/JobTable.vue')
const MagicEightBall = () => import('./views/MagicEightBall.vue')
const Schedule = () => import('./views/Schedule.vue')

const routes:RouteRecordRaw[] = [
  {
    component: JobTable,
    path: '/',
  },
  {
    component: Schedule,
    path: '/schedule',
  },
  {
    component: Configs,
    path: '/configs',
  },
  {
    component: MagicEightBall,
    path: '/8ball',
  }
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
})

export default router
