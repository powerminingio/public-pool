#!/usr/bin/env node

'use strict';

const net = require('net');
const path = require('path');

const ACT2_SIZE = 234;
const DEFAULT_TIMEOUT_MS = 5000;

function usage() {
  console.error(
    `Usage: ${path.basename(process.argv[1])} <host> <port>

Examples:
  ${path.basename(process.argv[1])} pool.example.com 23330
  ${path.basename(process.argv[1])} 192.0.2.10 23330

Optional:
  SV2_CHECK_TIMEOUT_MS=10000`,
  );
}

function fail(message, exitCode = 2) {
  console.error(`UNKNOWN: ${message}`);
  process.exit(exitCode);
}

const host = process.argv[2];
const port = Number(process.argv[3]);
const timeoutMs = Number(
  process.env.SV2_CHECK_TIMEOUT_MS || DEFAULT_TIMEOUT_MS,
);

if (!host || !Number.isInteger(port) || port < 1 || port > 65535) {
  usage();
  process.exit(64);
}

if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
  fail('SV2_CHECK_TIMEOUT_MS must be a positive integer');
}

let Sv2NoiseInitiator;

const moduleCandidates = [
  path.resolve(process.cwd(), 'dist/models/sv2/sv2-noise'),
  path.resolve(__dirname, 'dist/models/sv2/sv2-noise'),
  path.resolve(__dirname, '../dist/models/sv2/sv2-noise'),
];

for (const candidate of [...new Set(moduleCandidates)]) {
  try {
    ({ Sv2NoiseInitiator } = require(candidate));
    break;
  } catch (error) {
    if (error.code !== 'MODULE_NOT_FOUND') {
      fail(`cannot load ${candidate}: ${error.message}`);
    }
  }
}

if (!Sv2NoiseInitiator) {
  fail(
    'cannot find dist/models/sv2/sv2-noise; run "npm run build" in the public-pool repository first',
  );
}

async function main() {
  const initiator = new Sv2NoiseInitiator();
  const act1 = await initiator.generateAct1();

  const socket = net.createConnection({ host, port });

  let response = Buffer.alloc(0);
  let finished = false;

  const finish = (exitCode, message, useStderr = false) => {
    if (finished) {
      return;
    }

    finished = true;
    socket.destroy();

    if (useStderr) {
      console.error(message);
    } else {
      console.log(message);
    }

    process.exitCode = exitCode;
  };

  socket.setTimeout(timeoutMs);

  socket.once('connect', () => {
    socket.write(act1);
  });

  socket.on('data', chunk => {
    if (finished) {
      return;
    }

    response = Buffer.concat([response, chunk]);

    if (response.length < ACT2_SIZE) {
      return;
    }

    try {
      const { certificate } = initiator.processAct2(
        response.subarray(0, ACT2_SIZE),
      );

      const now = Math.floor(Date.now() / 1000);
      const remainingSeconds = certificate.notValidAfter - now;

      const validNow =
        certificate.validFrom <= now &&
        now < certificate.notValidAfter;

      const result = {
        status: validNow ? 'OK' : 'CRITICAL',
        endpoint: `${host}:${port}`,
        checkedAt: new Date(now * 1000).toISOString(),
        version: certificate.version,
        validFrom: new Date(
          certificate.validFrom * 1000,
        ).toISOString(),
        notValidAfter: new Date(
          certificate.notValidAfter * 1000,
        ).toISOString(),
        validNow,
        remainingSeconds,
      };

      finish(
        validNow ? 0 : 1,
        JSON.stringify(result, null, 2),
        !validNow,
      );
    } catch (error) {
      finish(
        2,
        `UNKNOWN: failed to decode SV2 Noise Act 2: ${error.message}`,
        true,
      );
    }
  });

  socket.once('timeout', () => {
    finish(
      2,
      `UNKNOWN: timeout after ${timeoutMs} ms connecting to ${host}:${port}`,
      true,
    );
  });

  socket.once('end', () => {
    if (!finished) {
      finish(
        2,
        `UNKNOWN: connection closed after ${response.length} bytes; expected ${ACT2_SIZE} bytes of SV2 Noise Act 2`,
        true,
      );
    }
  });

  socket.once('error', error => {
    finish(
      2,
      `UNKNOWN: ${host}:${port}: ${error.message}`,
      true,
    );
  });
}

main().catch(error => {
  fail(error.stack || error.message);
});
