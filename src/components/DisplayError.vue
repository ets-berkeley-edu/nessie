<template>
  <div>
    <div v-for="error in errors" :key="error.id" class="has-error">
      <b-link
        v-if="error.stack"
        v-b-popover.hover="error.stack"
        active-class="rightbottom"
        title="Error">
        <i class="has-error fas fa-exclamation-triangle"></i>
      </b-link>
      {{ error.message }}
      [<b-link id="dismiss-error" @click="dismissError(error.id)">dismiss</b-link>]
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

<style scoped>
.has-error {
  color: red;
}
</style>
