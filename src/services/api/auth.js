const mocks = {
  auth: { POST: { token: 'a-mocked-token' } },
  'user/me': { GET: { name: 'Joe', title: 'Sir' } },
};

// TODO: What are the best practices w.r.t abstractions for API requests
const authApi = ({ url, method }) => new Promise((resolve, reject) => {
  setTimeout(() => {
    try {
      resolve(mocks[url][method || 'GET']);
    } catch (err) {
      reject(new Error(err));
    }
  }, 1000);
});

export default authApi;
