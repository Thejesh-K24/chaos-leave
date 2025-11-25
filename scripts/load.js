import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  vus: Number(__ENV.USERS || 150),
  duration: __ENV.DUR || '3m',
};

export default function () {
  const url = __ENV.URL;

  // CHAOS env has full string OR we build it from LAT/ERR/CPU
  let chaos = __ENV.CHAOS || "";
  if (!chaos) {
    const parts = [];
    if (__ENV.LAT) parts.push(`lat:${__ENV.LAT}`);
    if (__ENV.ERR) parts.push(`err:${__ENV.ERR}`);
    if (__ENV.CPU) parts.push(`cpu:${__ENV.CPU}`);
    chaos = parts.join(',');
  }

  const fullUrl = chaos ? `${url}?chaos=${encodeURIComponent(chaos)}` : url;

  http.get(fullUrl, { timeout: '60s' });

  // pacing – you can tune this; the paper typically cares about concurrency,
  // not insane RPS, so sleep(1) is fine to control cost.
  sleep(1);
}
