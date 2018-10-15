<template>
  <div class="footer-container">
    <div>
      <img src="@/assets/uc-berkeley-logo.svg"/>
    </div>
    <div class="footer-copyright">
      &copy; 2018 The Regents of the University of California
    </div>
  </div>
</template>

<script>
export default {
  name: 'Footer'
};
</script>

<style scoped lang="scss">
.footer-container {
  align-items: flex-start;
  align-self: end;
  display: flex;
  flex-direction: row;
  font-weight: 300;
  justify-content: space-between;
  padding-top: 20px;
}

.footer-copyright {
  display: inline-block;
  margin: 0 20px 0 20px;
  text-align: right;
  white-space: nowrap;
}
</style>
