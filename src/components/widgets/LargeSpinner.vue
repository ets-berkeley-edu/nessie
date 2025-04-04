<template>
  <div class="loader" />
</template>

<style scoped>
/* https://css-loaders.com/spinner */
.loader {
  animation: l24 1s infinite linear;
  aspect-ratio: 1;
  border: 8px solid #0000;
  border-radius: 50%;
  border-right-color: #546e7a;
  bottom: 0;
  left: 0;
  margin: auto;
  overflow: show;
  position: fixed;
  right: 0;
  top: 0;
  width: 72px;
  z-index: 999;
}
.loader:before,
.loader:after {
  content: "";
  position: absolute;
  inset: -8px;
  border-radius: 50%;
  border: inherit;
  animation: inherit;
  animation-duration: 2s;
}
.loader:after {
  animation-duration: 4s;
}
@keyframes l24 {
  100% {transform: rotate(1turn)}
}
</style>
