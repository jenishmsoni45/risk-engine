// One step of the measurement plan (design.md section 12, D40o): requests at a constant arrival
// rate for a fixed time, latency measured here, at the client. bench/run.py runs one step per
// call and reads the summary this writes.
//
// Environment:
//   TARGET    decisions (POST /v1/decisions), transfers (the stub's POST /transfers) or
//             hot (POST /v1/decisions, every request to the one receiver HOT_RECEIVER, from
//             senders drawn at random: D50)
//   BASE_URL  the API's or the stub's address
//   RATE      requests a second;  DURATION  the step's length, as k6 spells it (3m)
//   ACCOUNTS  how many accounts senders and receivers are drawn from
//   RUN_ID    a prefix that makes every requestId and Idempotency-Key new
//   SUMMARY   where the JSON summary is written

import http from 'k6/http';

const TARGET = __ENV.TARGET;
const BASE_URL = __ENV.BASE_URL;
const RATE = parseInt(__ENV.RATE, 10);
const DURATION = __ENV.DURATION;
const ACCOUNTS = parseInt(__ENV.ACCOUNTS, 10);
const RUN_ID = __ENV.RUN_ID;
const HOT_RECEIVER = __ENV.HOT_RECEIVER;
const SUMMARY = __ENV.SUMMARY;

if (!['decisions', 'transfers', 'hot'].includes(TARGET)) {
  throw new Error(`TARGET must be decisions, transfers or hot, got ${TARGET}`);
}
if (TARGET === 'hot' && !HOT_RECEIVER) {
  throw new Error('TARGET=hot needs HOT_RECEIVER');
}

// What counts as an answer rather than an error: a decision from the API; from the stub, a
// posting (201) or a refusal by a rule (403). The stub's 503s, its backup answer included, are
// errors here; bench/run.py reads the timeout rate from the stub's own counters.
http.setResponseCallback(TARGET === 'transfers' ? http.expectedStatuses(201, 403) : http.expectedStatuses(200));

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'p(50)', 'p(90)', 'p(99)', 'max', 'count'],
  scenarios: {
    step: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      // Enough virtual users for each request to take a second at this rate; past that, k6
      // drops iterations, which run.py counts as not holding the rate.
      preAllocatedVUs: Math.max(20, Math.ceil(RATE / 10)),
      maxVUs: Math.max(50, RATE),
    },
  },
};

// Account n as a canonical UUID; n is never 0.
function account(n) {
  return `00000000-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;
}

function pick() {
  return 1 + Math.floor(Math.random() * ACCOUNTS);
}

export default function () {
  let from = pick();
  let to = pick();
  while (to === from) {
    to = pick();
  }
  const sender = account(from);
  const receiver = TARGET === 'hot' ? HOT_RECEIVER : account(to);
  const amount = 100 + Math.floor(Math.random() * 9900);
  const id = `${RUN_ID}-${__VU}-${__ITER}`;
  const headers = { 'Content-Type': 'application/json' };

  if (TARGET === 'transfers') {
    headers['Idempotency-Key'] = id;
    http.post(
      `${BASE_URL}/transfers`,
      JSON.stringify({ fromAccountId: sender, toAccountId: receiver, amount, currency: 'CAD' }),
      { headers },
    );
    return;
  }
  http.post(
    `${BASE_URL}/v1/decisions`,
    JSON.stringify({
      requestId: id,
      fromAccountId: sender,
      toAccountId: receiver,
      amount,
      currency: 'CAD',
      requestedAt: new Date().toISOString(),
    }),
    { headers },
  );
}

export function handleSummary(data) {
  const duration = data.metrics.http_req_duration.values;
  const failed = data.metrics.http_req_failed.values;
  const line = `${TARGET} at ${RATE}/s: p50 ${duration['p(50)'].toFixed(1)} ms, p99 ${duration['p(99)'].toFixed(1)} ms, failed ${(100 * failed.rate).toFixed(2)}%\n`;
  return { [SUMMARY]: JSON.stringify(data, null, 2), stdout: line };
}
