<template>
  <div>
    <h1>Status</h1>
    <h2>Ping</h2>
    <div>
      <ul>
        <li>App: {{ ping.app }}</li>
        <li>RDS: {{ ping.rds }}</li>
        <li>Redshift: {{ ping.redshift }}</li>
      </ul>
    </div>
    <h2>Config</h2>
    <div>
      <ul>
        <li>Nessie environment: {{ nessieEnv }}</li>
        <li>EB environment: {{ ebEnvironment }}</li>
        <li>Current Enrollment Term: {{ currentEnrollmentTerm }}</li>
        <li>Current Enrollment Term ID: {{ currentEnrollmentTermId }}</li>
        <li>EDL feature flag: {{ featureFlagEnterpriseDataLake }}</li>
      </ul>
    </div>
    <h2>Version</h2>
    <div>
      <ul>
        <li>Version: {{ version.version }}</li>
        <li>
          Build
          <ul>
            <li>Artifact: {{ version.build && version.build.artifact ? version.build.artifact : '--' }}</li>
            <li>Git commit: {{ version.build && version.build.gitCommit ? version.build.gitCommit : '--' }}</li>
          </ul>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import Context from '@/mixins/Context'

export default {
  mixins: [Context]
}
</script>
