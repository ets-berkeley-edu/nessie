<template>
  <v-carousel cycle interval="10000">
    <v-carousel-item
      v-for="slide in slides"
      :key="slide"
      :src="slide"
      transition="fade-transition"
      reverse-transition="fade-transition"
      alt="Image of Loch Ness, in Scotland"
      cover
    />
  </v-carousel>
</template>

<script setup>
import castleOnShore from '@/assets/loch-ness-castle-on-shore.jpg'
import lochNessMonster from '@/assets/loch-ness-monster.jpg'
import lochNessViewFromMountains from '@/assets/loch-ness-view-from-mountains.jpg'

const slides = [castleOnShore, lochNessMonster, lochNessViewFromMountains]
</script>

<style>
.v-carousel .v-window-item {
  position: absolute;
  top: 0;
  width: 100%;
}
</style>
