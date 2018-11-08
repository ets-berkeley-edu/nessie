<template>
  <div>
    <RunJob/>
    <JobStatus/>
  </div>
</template>

<script>
import RunJob from '@/components/jobs/RunJob.vue';
import JobStatus from '@/components/jobs/JobStatus.vue';

export default {
  components: {
    RunJob,
    JobStatus
  }
};
</script>
