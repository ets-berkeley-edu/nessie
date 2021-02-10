<script>
import { mapGetters } from 'vuex'

export default {
  name: 'Context',
  computed: {
    ...mapGetters('context', [
      'apiBaseUrl',
      'currentEnrollmentTermId',
      'currentEnrollmentTerm',
      'currentEnrollmentTermId',
      'ebEnvironment',
      'errors',
      'featureFlagEnterpriseDataLake',
      'nessieEnv',
      'ping',
      'version'
    ])
  }
}
</script>
