import {get, includes, noop} from 'lodash'
import {DateTime} from 'luxon'
import {nextTick} from 'vue'
import {useContextStore} from '@/stores/context'

export function initializeAxios(app: object, axios: object) {
  axios.defaults.withCredentials = true
  axios.interceptors.response.use(
    (response: object) => response.headers['content-type'] === 'application/json' ? response.data : response,
    (error: object) => {
      const errorStatus = get(error, 'response.status')
      if (includes([401, 403], errorStatus)) {
        const isAuthenticated = useContextStore().currentUser.isAuthenticated
        if (!isAuthenticated) {
          useContextStore().setApplicationState(errorStatus, 'Your session has expired')
        }
        return Promise.reject(error)
      } else {
        return Promise.reject(error)
      }
    })
}

export function putFocusNextTick(id: string, cssSelector?: string) {
  const callable = () => {
    let el = document.getElementById(id)
    el = el && cssSelector ? el.querySelector(cssSelector) : el
    if (el) {
      el.focus()
    }
    return !!el
  }
  nextTick(() => {
    let counter = 0
    const job: object = setInterval(() => (callable() || ++counter > 3) && clearInterval(job), 500)
  }).then(noop)
}

export function toFormatFromISO(isoString: string, format: string): string {
  // See https://moment.github.io/luxon/#/formatting?id=table-of-tokens
  return DateTime.fromISO(isoString).toFormat(format)
}

export function toFormatFromJsDate(jsDate: Date, format: string): string {
   // See https://moment.github.io/luxon/#/formatting?id=table-of-tokens
   return DateTime.fromJSDate(jsDate).toFormat(format)
}
