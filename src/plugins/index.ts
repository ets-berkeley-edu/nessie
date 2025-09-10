import type {App} from 'vue'
import {createPinia} from 'pinia'
import vuetify from './vuetify'
import axios from '@/plugins/axios'

export function registerPlugins (app: App) {
  app
    .use(axios, {baseUrl: import.meta.env.VITE_APP_API_BASE_URL})
    .use(createPinia())
    .use(vuetify)
}
