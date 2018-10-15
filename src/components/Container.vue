<template>
  <div class="container">
    <slot/>
  </div>
</template>

<script>
export default {
  name: 'Container'
};
</script>

<style scoped lang="scss">
.center-container {
  display: flex;
  align-items: center;
  flex-direction: column;
}
</style>
