import mitt from 'mitt'
import {defineStore} from 'pinia'
import {get} from 'lodash'
import {nextTick} from 'vue'
import {putFocusNextTick} from '@/utils'
import router from '@/router'

export const useContextStore = defineStore('context', {
  state: () => ({
    config: undefined,
    currentUser: {},
    eventHub: mitt(),
    isLoading: false,
    screenReaderAlert: {
      message: '',
      politeness: 'polite'
    }
  }),
  actions: {
    alertScreenReader(message: string, politeness?: string) {
      this.screenReaderAlert.message = ''
      nextTick(() => {
        this.screenReaderAlert = {
          message: message,
          politeness: politeness || 'polite'
        }
      })
    },
    loadingComplete(focusTarget?: string) {
      this.isLoading = false
      const route = router.currentRoute
      if (!get(route, 'value.meta.announcer.skip')) {
        this.screenReaderAlert.message = `${String(get(route, 'value.name', ''))} page has loaded.`
        this.screenReaderAlert.politeness = 'assertive'
      }
      putFocusNextTick(focusTarget || 'page-title')
    },
    loadingStart(route?: Object) {
      this.isLoading = true
      if (!get(route, 'meta.announcer.skip')) {
        this.screenReaderAlert.message = `${String(get(route, 'name', ''))} page is loading.`
      }
    },
    setApplicationState(status: number, message?: any, stacktrace?: any) {
      this.applicationState = {message, stacktrace, status}
    },
    setConfig(config: any) {
      this.config = config
    },
    setCurrentUser(user: any) {
      this.currentUser = user
      this.eventHub.emit('current-user-update')
    }
  }
})
