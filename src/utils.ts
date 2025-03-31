import {get, includes, noop} from 'lodash'
import {nextTick} from 'vue'
import {useContextStore} from '@/stores/context'

export function initializeAxios(app: any, axios: any) {
  axios.defaults.withCredentials = true
  axios.interceptors.response.use(
    (response: any) => response.headers['content-type'] === 'application/json' ? response.data : response,
    (error: any) => {
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
    el && el.focus()
    return !!el
  }
  nextTick(() => {
    let counter = 0
    const job:any = setInterval(() => (callable() || ++counter > 3) && clearInterval(job), 500)
  }).then(noop)
}
