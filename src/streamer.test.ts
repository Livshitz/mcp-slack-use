import { test, expect, beforeEach, afterEach } from 'bun:test';
import { SlackStreamer, finalPayload, MARKDOWN_BLOCK_LIMIT } from './streamer.ts';
import { mdToMrkdwn } from './ingress.ts';

const TABLE = '## Title\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n**bold**';
let calls: { method: string; body: Record<string, string> }[] = [];
const realFetch = globalThis.fetch;

beforeEach(() => {
  calls = [];
  process.env.SLACK_BOT_TOKEN = 'xoxb-test';
  globalThis.fetch = (async (url: string, init: any) => {
    const method = String(url).split('/api/')[1];
    calls.push({ method, body: Object.fromEntries(new URLSearchParams(init.body)) });
    return new Response(JSON.stringify({ ok: true, ts: '1.1', team_id: 'T', user_id: 'U' }));
  }) as any;
});
afterEach(() => { globalThis.fetch = realFetch; });

test('finalPayload: markdown block + mrkdwn fallback text', () => {
  const p = finalPayload(TABLE, 'fallback');
  expect(p).toEqual({ text: 'fallback', blocks: [{ type: 'markdown', text: TABLE }] });
});

test('finalPayload: over the 12k cap falls back to text only', () => {
  expect(finalPayload('x'.repeat(MARKDOWN_BLOCK_LIMIT + 1), 'fb')).toEqual({ text: 'fb' });
  expect(finalPayload('x'.repeat(MARKDOWN_BLOCK_LIMIT), 'fb').blocks).toBeDefined();
});

test('streamed finish: chat.update carries raw markdown block (table kept) + mrkdwn text', async () => {
  const s = new SlackStreamer({ channel: 'C1', threadTs: '0.1', transform: mdToMrkdwn });
  s.feed({ type: 'text_delta', text: TABLE });
  await s.finish();
  const upd = calls.find((c) => c.method === 'chat.update')!;
  expect(JSON.parse(upd.body.blocks)).toEqual([{ type: 'markdown', text: TABLE }]);
  expect(upd.body.text).toBe(mdToMrkdwn(TABLE));
});

test('flat finish: postMessage carries the markdown block', async () => {
  const s = new SlackStreamer({ channel: 'C1', transform: mdToMrkdwn });
  s.feed({ type: 'text_delta', text: TABLE });
  await s.finish();
  const post = calls.find((c) => c.method === 'chat.postMessage')!;
  expect(JSON.parse(post.body.blocks)[0].text).toBe(TABLE);
});

test('rejected markdown block: retries text-only so the reply is not lost (flat + streamed)', async () => {
  globalThis.fetch = (async (url: string, init: any) => {
    const method = String(url).split('/api/')[1];
    const body = Object.fromEntries(new URLSearchParams(init.body));
    calls.push({ method, body });
    const ok = !body.blocks;
    return new Response(JSON.stringify(ok ? { ok, ts: '1.1', team_id: 'T', user_id: 'U' } : { ok, error: 'invalid_blocks' }));
  }) as any;
  const flat = new SlackStreamer({ channel: 'C1', transform: mdToMrkdwn });
  flat.feed({ type: 'text_delta', text: TABLE });
  expect(await flat.finish()).toBe('1.1');
  const posts = calls.filter((c) => c.method === 'chat.postMessage');
  expect(posts.length).toBe(2);
  expect(posts[1].body.blocks).toBeUndefined();
  expect(posts[1].body.text).toBe(mdToMrkdwn(TABLE));

  calls = [];
  const st = new SlackStreamer({ channel: 'C1', threadTs: '0.1', transform: mdToMrkdwn });
  st.feed({ type: 'text_delta', text: TABLE });
  await st.finish();
  const upds = calls.filter((c) => c.method === 'chat.update');
  expect(upds.length).toBe(2);
  expect(upds[1].body.blocks).toBeUndefined();
});
