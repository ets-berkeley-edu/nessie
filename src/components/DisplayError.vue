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
import Context from '@/mixins/Context';

export default {
  name: 'DisplayError',
  mixins: [Context],
  methods: {
    dismissError(id) {
      store.commit('context/dismissError', id);
    }
  }
};
</script>

<style scoped lang="scss">
.has-error {
  color: red;
}
</style>
