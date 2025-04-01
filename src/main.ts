
import App from './App.vue'
import axios from 'axios'
import CustomEvents from 'highcharts-custom-events'
import highchartsDumbbell from 'highcharts/modules/dumbbell'
import HC_more from 'highcharts/highcharts-more'
import Highcharts from 'highcharts'
import HighchartsVue from 'highcharts-vue'
import router from '@/router'

import {createApp} from 'vue'
import {get, trim} from 'lodash'
import {initializeAxios} from './utils'
import {registerPlugins} from '@/plugins'
import {useContextStore} from '@/stores/context'

const app = createApp(App)
app.use(HighchartsVue)

registerPlugins(app)
initializeAxios(app, axios)

HC_more(Highcharts)
highchartsDumbbell(Highcharts)
CustomEvents(Highcharts)

// Globals
app.config.globalProperties.$isInIframe = !!window.parent.frames.length
app.config.globalProperties.$ready = (focusTarget?: string) => useContextStore().loadingComplete(focusTarget)

const apiBaseUrl = import.meta.env.VITE_APP_API_BASE_URL
const contextStore = useContextStore()

axios.get(`${apiBaseUrl}/api/user/profile`).then(data => {
  contextStore.setCurrentUser(data)

  axios.get(`${apiBaseUrl}/api/config`).then(data => {
    contextStore.setConfig({
      ...data,
      apiBaseUrl,
      isVueAppDebugMode: trim(import.meta.env.VITE_APP_DEBUG).toLowerCase() === 'true'
    })
    contextStore.setVersion()

    app.use(router).config.errorHandler = function (error, vm, info) {
      const message = get(error, 'message') || info
      const stacktrace = get(error, 'stack', null)
      console.log(`\n${message}\n${stacktrace}\n`)
      useContextStore().setApplicationState(500, message, stacktrace)
    }
    app.mount('#app')
  })
})
