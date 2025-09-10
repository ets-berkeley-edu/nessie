import mitt from 'mitt'
import type {Handler} from 'mitt'
import {defineStore} from 'pinia'
import {get} from 'lodash'
import {nextTick} from 'vue'
import {putFocusNextTick} from '@/utils'
import router from '@/router'

import {getVersion} from '@/api/status'

export const useContextStore = defineStore('context', {
  state: () => ({
    config: undefined,
    currentUser: {},
    eventHub: mitt(),
    isLoading: false,
    screenReaderAlert: {
      message: '',
      politeness: 'polite'
    },
    version: undefined
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
    broadcast(eventType, data?) {
      this.eventHub.emit(eventType, data)
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
    loadingStart(route?: object) {
      this.isLoading = true
      if (!get(route, 'meta.announcer.skip')) {
        this.screenReaderAlert.message = `${String(get(route, 'name', ''))} page is loading.`
      }
    },
    setApplicationState(status: number, message?: string, stacktrace?: string) {
      this.applicationState = {message, stacktrace, status}
    },
    setConfig(config: object) {
      this.config = config
    },
    setCurrentUser(user: object) {
      this.currentUser = user
      this.eventHub.emit('current-user-update')
    },
    setEventHandler(type: string, handler: Handler) {
      this.eventHub.on(type, handler)
    },
    setVersion() {
      getVersion().then(version => {
        this.version = version
      })
    },
  }
})
