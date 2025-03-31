<template>
  <div class="loading-spinner-large">
    <i class="fas fa-sync fa-spin fa-5x"></i>
    <span role="alert" aria-live="polite" class="sr-only">Loading...</span>
  </div>
</template>

<style>
.loading-spinner-large {
  position: fixed;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  height: 2em;
  margin: auto;
  overflow: show;
  width: 2em;
  z-index: 999;
}
</style>
