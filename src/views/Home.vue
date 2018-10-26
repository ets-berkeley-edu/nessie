<template>
  <div role="tablist">
    <JobMetadata/>
    <Schedule/>
  </div>
</template>

<script>
import JobMetadata from '@/components/JobMetadata.vue';
import Schedule from '@/components/Schedule.vue';

export default {
  components: {
    JobMetadata,
    Schedule
  }
};
</script>
