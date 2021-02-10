<script>
import { mapGetters } from 'vuex'

export default {
  name: 'Context',
  computed: {
    ...mapGetters('context', [
      'apiBaseUrl',
      'errors',
      'ping',
      'version'
    ])
  }
}
</script>
