module.exports = {
    apps: [
      // Master instance
      {
        name: 'master',
        script: './dist/main.js',
        instances: 1,
        env: {
          MASTER: 'true',
          API_PORT: "3335"
        },
        time: true
      },
      // Worker instances
      {
        name: 'worker-1',
        script: './dist/main.js',
        instances: 1,
        env: {
          MASTER: 'false',
          API_PORT: "3336"
        },
        time: true
      },
      {
        name: 'worker-2',
        script: './dist/main.js',
        instances: 1,
        env: {
          MASTER: 'false',
          API_PORT: "3337"
        },
        time: true
      },
      // API aggregator
      {
        name: 'pool-api-agg',
        script: './tools/api-aggregator.js',
        env: {
          AGG_PORT: '3334',
          MASTER_API: 'http://127.0.0.1:3335',
          WORKER_APIS: 'http://127.0.0.1:3336,http://127.0.0.1:3337'
        }
      },
    ],
  };
