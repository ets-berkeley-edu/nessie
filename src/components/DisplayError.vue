<template>
  <div>
    <div class="has-error" v-for="error in errors" v-bind:key="error.id">
      <b-link v-b-popover.hover="error.stack"
              active-class="rightbottom"
              title="Error"
              v-if="error.stack">
        <i class="has-error fas fa-exclamation-triangle"></i>
      </b-link>
      {{error.message}}
      [<b-link id="dismiss-error" v-on:click="dismissError(error.id)">dismiss</b-link>]
    </div>
  </div>
</template>

<script>
import store from '@/store';

export default {
  name: 'DisplayError',
  data() {
    return {
      errors: store.getters.errors
    };
  },
  methods: {
    dismissError(id) {
      store.commit('dismissError', id);
    }
  }
};
</script>

<style scoped lang="scss">
.has-error {
  color: red;
}
</style>
