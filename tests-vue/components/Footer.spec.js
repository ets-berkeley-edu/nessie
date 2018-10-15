import { shallowMount } from '@vue/test-utils'
import Footer from '@/components/Footer.vue'

describe('Footer.vue', () => {
  it('verifies boilerplate text', () => {
    const wrapper = shallowMount(Footer);
    expect(wrapper.text()).toContain(2018);
  });
});
