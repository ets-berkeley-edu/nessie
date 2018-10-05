import { shallowMount } from '@vue/test-utils'
import Footer from '@/components/Footer.vue'

describe('Footer.vue', () => {
  it('displays Hello World in footer', () => {
    const msg = 'This is a footer'
    const wrapper = shallowMount(Footer)
    expect(wrapper.text()).toContain(msg)
  })
})
