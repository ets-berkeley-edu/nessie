<template>
  <div class="main">
    <slot />
  </div>
</template>

<script>
export default {
  name: 'Container'
};
</script>

<style scoped lang="scss">
.main {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  padding: 0 30px 0 30px;
  width: 100%;
}
</style>
