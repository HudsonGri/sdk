export const actor = {
  restore() {},
  async event() { throw new Error('sdk deterministic poison'); },
  checkpoint() { return new Uint8Array(); }
};
export default {async fetch() { return new Response('poison fixture'); }};
