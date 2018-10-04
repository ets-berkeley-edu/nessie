<template>
  <div class="sq-footer">
    Made with love at <a target="_blank" href="https://www.ets.berkeley.edu/">ETS</a>.
  </div>
</template>

<style scoped>
  .sq-footer {
    background-color: rgb(183, 72, 14);
    height: 20px;
    font-size: 12px;
    display: flex;
    justify-content: flex-end;
    color: white;
    align-items: center;
    padding-right: 1em;
  }
  a {
    color:white;
  }
</style>

<script>
export default {
  name: 'footer',
};
</script>
