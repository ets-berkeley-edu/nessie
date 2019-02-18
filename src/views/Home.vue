<template>
  <div>
    <RunJob />
    <JobStatus />
  </div>
</template>

<script>
import JobStatus from '@/components/jobs/JobStatus';
import RunJob from '@/components/jobs/RunJob';

export default {
  components: {
    RunJob,
    JobStatus
  }
};
</script>
