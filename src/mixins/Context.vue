<script>
import {mapGetters} from 'vuex'

export default {
  name: 'Context',
  computed: {
    ...mapGetters('context', [
      'apiBaseUrl',
      'errors',
      'loading',
      'ping',
      'version'
    ])
  }
}
</script>
