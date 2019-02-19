<template>
  <div>
    <form @submit.prevent="casLogin">
      <button id="cas-log-in" class="btn btn-default btn-primary splash-btn-sign-in">Sign In</button>
    </form>
  </div>
</template>
<script>
import { getCasLoginURL } from '@/api/user';

export default {
  methods: {
    casLogin() {
      getCasLoginURL().then(data => {
        window.location = data.casLoginURL;
      });
    }
  }
};
</script>
