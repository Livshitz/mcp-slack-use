import { resolve, basename, extname } from 'node:path';
import { readFileSync, existsSync } from 'node:fs';
import { json } from 'itty-router';
import { RouterWrapper } from 'edge.libx.js/build/main.js';
import { augmentMcpWithSkillResource } from './mcp/with-skill-resource.ts';
import { requireToken, optionalUserToken, slackApi } from './slack-api.ts';
import { buildPollBlocks, type PollSpec, type PollKind, type PollOption } from './poll-blocks.ts';
import { renderCard, CARD_COMPONENTS } from './card-blocks.ts';
import { slimChannels, slimHistory, slimSearch } from './slim.ts';
import { inlineOrSpool } from './spool.ts';
import { registerDmUserRoutes } from './routes-dm.ts';

function errMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

const channelCache = new Map<string, { data: unknown; ts: number }>();
const CHANNEL_CACHE_TTL = 5 * 60 * 1000;

function channelCacheKey(types: string, limit: number, full: boolean, cursor?: string) {
  return `${types}:${limit}:${full}:${cursor ?? ''}`;
}

/** itty-router sometimes leaves `query` empty for Request-based fetch; fall back to URL. */
function qp(req: { url: string; query?: Record<string, unknown> }, key: string): string | undefined {
  const q = req.query?.[key];
  if (q !== undefined && q !== null && String(q) !== '') return String(q);
  try {
    return new URL(req.url, 'http://_').searchParams.get(key) ?? undefined;
  } catch {
    return undefined;
  }
}

function truthyFull(req: { url: string; query?: Record<string, unknown> }): boolean {
  const v = qp(req, 'full');
  return v === 'true' || v === '1' || v === 'yes';
}

export function createSlackMcp() {
  const base = RouterWrapper.getNew('', {
    origin: '*',
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'Authorization'],
  });

  const { router } = base;

  base.describeMCP('/slack/message', 'POST', {
    description:
      'Post as the Slack app bot (SLACK_BOT_TOKEN must be xoxb- Bot User OAuth; xoxp- posts as that human). Channel C…/G… or DM D…. New DM: GET /slack/user/by-email if needed, POST /slack/dm/open with U…, then post with channel = channel_id. Raw @name or email will fail. ' +
      'FORMATTING: `text` is Slack mrkdwn, NOT markdown — Slack does NOT render markdown tables (pipe `|` rows show as raw text). Wrap any table/aligned data in a triple-backtick code fence (monospace, space-pad the columns) and use *single asterisks* for bold (not **double**), _underscores_ for italic. ' +
      'Returns { channel, ts } — keep the ts to later EDIT this message via post_slack_update or remove it via post_slack_delete (this app can edit/delete its own posts — bot or user identity; prefer editing over posting a correction).',
    params: {
      body: {
        description:
          '{ channel, text, optional thread_ts, optional blocks } — channel is C/G/D id or from dm/open. text = Slack mrkdwn: *bold* (single *), code-fence tables (no markdown pipe-tables — they render as raw `|`).',
        type: 'object',
      },
    },
    annotations: { destructiveHint: false },
  });
  router.post('/slack/message', async (req) => {
    try {
      const token = requireToken();
      const body = (await req.json()) as Record<string, unknown>;
      const channel = body.channel;
      const text = body.text;
      if (typeof channel !== 'string' || !channel)
        return json({ ok: false, error: 'channel is required' }, { status: 400 });
      if (typeof text !== 'string' || !text)
        return json({ ok: false, error: 'text is required' }, { status: 400 });
      const payload: Record<string, unknown> = { channel, text };
      if (typeof body.thread_ts === 'string') payload.thread_ts = body.thread_ts;
      if (body.blocks != null) payload.blocks = body.blocks;
      const data = await slackApi<{
        channel?: string;
        ts?: string;
        message?: { text?: string };
      }>(token, 'chat.postMessage', payload);
      if (!data.ok) return json(data, { status: 400 });
      return json(data);
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  // chat.update/chat.delete only succeed for the token that AUTHORED the message.
  // In unclaw the bot (xoxb) and the agent's user account (xoxp) BOTH post — so try the
  // requested identity first, then fall back to the other token on an author-mismatch error.
  // Errors that mean "this token can't act here, but the other token might" — wrong author
  // (cant_update/delete_message), or the token can't see the conversation at all
  // (channel_not_found/not_in_channel, e.g. a DM the bot isn't a member of).
  const AUTHOR_MISMATCH = new Set([
    'cant_update_message', 'cant_delete_message', 'message_not_found',
    'edit_window_closed', 'message_not_authored_by_user',
    'channel_not_found', 'not_in_channel',
  ]);
  async function editWithFallback<T extends { ok: boolean; error?: string }>(
    method: string, payload: Record<string, unknown>, asUser?: boolean,
  ): Promise<T> {
    const bot = requireToken();
    const user = optionalUserToken();
    // Order tokens by requested identity; dedupe when no distinct user token exists.
    const order = asUser && user ? [user, bot] : [bot, ...(user ? [user] : [])];
    let last: T | undefined;
    for (const token of order) {
      last = await slackApi<T>(token, method, payload) as T;
      if (last.ok || !AUTHOR_MISMATCH.has(last.error ?? '')) return last;
    }
    return last as T;
  }

  base.describeMCP('/slack/update', 'POST', {
    description:
      'Edit a message previously posted by THIS app (chat.update) — correct/replace a prior post instead of adding a follow-up. Works for messages authored by either the bot (xoxb) or the agent\'s user account (xoxp); it auto-tries both, so you normally do NOT need as_user. Cannot edit other people\'s messages. ' +
      'FORMATTING: same as post_slack_message — `text` is Slack mrkdwn (*single-asterisk* bold, no markdown pipe-tables; wrap tables in a triple-backtick code fence).',
    params: {
      body: {
        description:
          '{ channel: string (C/G/D id, same channel the message is in), ts: string (the target message ts to edit), text?: string, blocks?: array, as_user?: boolean } — provide text and/or blocks (at least one). Passing blocks without text replaces the blocks. as_user=true tries the user token first (rarely needed; both tokens are tried automatically).',
        type: 'object',
      },
    },
    annotations: { destructiveHint: false },
  });
  router.post('/slack/update', async (req) => {
    try {
      const body = (await req.json()) as Record<string, unknown>;
      const channel = body.channel;
      const ts = body.ts;
      if (typeof channel !== 'string' || !channel)
        return json({ ok: false, error: 'channel is required' }, { status: 400 });
      if (typeof ts !== 'string' || !ts)
        return json({ ok: false, error: 'ts is required (the message timestamp to edit)' }, { status: 400 });
      const hasText = typeof body.text === 'string' && body.text;
      const hasBlocks = body.blocks != null;
      if (!hasText && !hasBlocks)
        return json({ ok: false, error: 'text and/or blocks is required' }, { status: 400 });
      const payload: Record<string, unknown> = { channel, ts };
      if (hasText) payload.text = body.text;
      if (hasBlocks) payload.blocks = body.blocks;
      const data = await editWithFallback<{ ok: boolean; error?: string; channel?: string; ts?: string; text?: string }>(
        'chat.update', payload, body.as_user === true,
      );
      if (!data.ok) return json(data, { status: 400 });
      return json(data);
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/delete', 'POST', {
    description:
      'Delete a message previously posted by THIS app (chat.delete) — remove an erroneous/stale post. Works for messages authored by either the bot (xoxb) or the agent\'s user account (xoxp); it auto-tries both. Cannot delete other people\'s messages.',
    params: {
      body: {
        description: '{ channel: string (C/G/D id), ts: string (the message ts to delete), as_user?: boolean (tries user token first; rarely needed) }',
        type: 'object',
      },
    },
    annotations: { destructiveHint: true },
  });
  router.post('/slack/delete', async (req) => {
    try {
      const body = (await req.json()) as Record<string, unknown>;
      const channel = body.channel;
      const ts = body.ts;
      if (typeof channel !== 'string' || !channel)
        return json({ ok: false, error: 'channel is required' }, { status: 400 });
      if (typeof ts !== 'string' || !ts)
        return json({ ok: false, error: 'ts is required (the message timestamp to delete)' }, { status: 400 });
      const data = await editWithFallback<{ ok: boolean; error?: string; channel?: string; ts?: string }>(
        'chat.delete', { channel, ts }, body.as_user === true,
      );
      if (!data.ok) return json(data, { status: 400 });
      return json(data);
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/poll', 'POST', {
    description:
      'Post an interactive poll or directed question to a Slack channel/DM. Renders clickable option buttons with a live vote tally and a Close button. Returns a pollId; the host app records votes (via Slack interactivity), updates the tally live, and closes the poll on deadline/quorum (poll) or first answer (question). Use kind="poll" for a multi-user vote, kind="question" to ask one person (set target_user to their U… id). channel is C/G/D id (resolve names/emails first).',
    params: {
      body: {
        description:
          '{ channel: string, title: string, options: Array<{label: string, description?: string}> (2-10), kind?: "poll"|"question" (default poll), multi?: boolean (poll only), target_user?: string (U… id, question only), thread_ts?: string, deadline_minutes?: number (auto-close + report results after N minutes), quorum?: number (auto-close once this many people have voted) }. A poll closes on deadline OR quorum (whichever first) OR manual Close; a question closes on first answer. On close the agent that created it is notified with the tally.',
        type: 'object',
      },
    },
    annotations: { destructiveHint: false },
  });
  router.post('/slack/poll', async (req) => {
    try {
      const token = requireToken();
      const body = (await req.json()) as Record<string, unknown>;
      const channel = body.channel;
      const title = body.title;
      const options = body.options as PollOption[] | undefined;
      if (typeof channel !== 'string' || !channel)
        return json({ ok: false, error: 'channel is required' }, { status: 400 });
      if (typeof title !== 'string' || !title)
        return json({ ok: false, error: 'title is required' }, { status: 400 });
      if (!Array.isArray(options) || options.length < 2 || options.length > 10)
        return json({ ok: false, error: 'options must be an array of 2-10 {label,description?}' }, { status: 400 });

      const kind: PollKind = body.kind === 'question' ? 'question' : 'poll';
      const spec: PollSpec = {
        pollId: crypto.randomUUID().slice(0, 8),
        title,
        options: options.map((o) => ({ label: String((o as any).label ?? ''), description: (o as any).description })),
        kind,
        multi: kind === 'poll' ? !!body.multi : false,
        targetUser: typeof body.target_user === 'string' ? body.target_user : undefined,
      };
      const payload: Record<string, unknown> = { channel, text: spec.title, blocks: buildPollBlocks(spec) };
      if (typeof body.thread_ts === 'string') payload.thread_ts = body.thread_ts;
      const data = await slackApi<{ channel?: string; ts?: string }>(token, 'chat.postMessage', payload);
      if (!data.ok) return json(data, { status: 400 });
      return json({ ok: true, pollId: spec.pollId, channel: data.channel ?? channel, ts: data.ts, kind: spec.kind, multi: spec.multi, targetUser: spec.targetUser, options: spec.options });
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/card', 'POST', {
    description:
      `Post a rich card (native Block Kit) to a Slack channel/DM/thread instead of a wall of text: tables, key-value fields, charts, images, buttons. Use when the answer is structured data or needs a click. Write a json-render spec using ONLY these components: ${CARD_COMPONENTS}. Buttons (not LinkButton) are clickable answers: the first click closes the card and you get notified with the choice, like a directed question. An invalid spec returns 400 with a readable error per issue — fix the spec and retry. channel is C/G/D id.`,
    params: {
      body: {
        description:
          '{ channel: string, spec: { root: string, elements: Record<id, { type, props, children: id[] }> }, thread_ts?: string, target_user?: string (U… id: only this user may click) }. Example spec: { root:"c", elements:{ c:{type:"Card",props:{title:"Signups"},children:["t","a"]}, t:{type:"Table",props:{headers:["Day","N"],rows:[["Mon","12"]]},children:[]}, a:{type:"Actions",props:{},children:["b"]}, b:{type:"Button",props:{id:"ok",label:"Approve",style:"primary"},children:[]} } }. Buttons must sit inside Actions.',
        type: 'object',
      },
    },
    annotations: { destructiveHint: false },
  });
  router.post('/slack/card', async (req) => {
    try {
      const token = requireToken();
      const body = (await req.json()) as Record<string, unknown>;
      const channel = body.channel;
      if (typeof channel !== 'string' || !channel)
        return json({ ok: false, error: 'channel is required' }, { status: 400 });
      const cardId = crypto.randomUUID().slice(0, 8);
      let card;
      try { card = renderCard(body.spec, cardId); }
      catch (e) { return json({ ok: false, error: `invalid spec: ${errMessage(e)}` }, { status: 400 }); }
      const payload: Record<string, unknown> = { channel, text: card.text, blocks: card.blocks };
      if (typeof body.thread_ts === 'string') payload.thread_ts = body.thread_ts;
      const data = await slackApi<{ channel?: string; ts?: string }>(token, 'chat.postMessage', payload);
      if (!data.ok) return json(data, { status: 400 });
      const targetUser = typeof body.target_user === 'string' ? body.target_user : undefined;
      return json({
        ok: true, channel: data.channel ?? channel, ts: data.ts,
        // Host contract (same as /slack/poll): register a directed question when the card has buttons.
        ...(card.options.length ? { pollId: cardId, kind: 'question', title: card.text.split('\n')[0].replace(/\*/g, '').slice(0, 150), targetUser, options: card.options, blocks: card.blocks } : {}),
      });
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/files', 'POST', {
    description:
      'Upload files to a Slack channel or thread. Reads files from local filesystem paths. Requires files:write bot scope.',
    params: {
      body: {
        description:
          '{ channel: string, files: Array<{path: string, filename?: string}>, thread_ts?: string, initial_comment?: string }',
        type: 'object',
      },
    },
    annotations: { destructiveHint: false },
  });
  router.post('/slack/files', async (req) => {
    try {
      const token = requireToken();
      const body = (await req.json()) as Record<string, unknown>;
      const channel = body.channel as string;
      const files = body.files as Array<{ path: string; filename?: string }>;
      if (!channel) return json({ ok: false, error: 'channel is required' }, { status: 400 });
      if (!Array.isArray(files) || !files.length)
        return json({ ok: false, error: 'files array is required and must not be empty' }, { status: 400 });

      const uploaded: Array<{ id: string; title: string }> = [];
      for (const f of files) {
        if (!existsSync(f.path))
          return json({ ok: false, error: `File not found: ${f.path}` }, { status: 400 });

        const fileData = readFileSync(f.path);
        const filename = f.filename ?? basename(f.path);
        const length = fileData.byteLength;

        const urlRes = await slackApi<{ upload_url: string; file_id: string }>(
          token, 'files.getUploadURLExternal', { filename, length },
        );
        if (!urlRes.ok) return json(urlRes, { status: 400 });

        const ext = extname(filename).toLowerCase();
        const mimeMap: Record<string, string> = {
          '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif',
          '.webp': 'image/webp', '.pdf': 'application/pdf', '.csv': 'text/csv',
          '.json': 'application/json', '.txt': 'text/plain', '.zip': 'application/zip',
        };
        const contentType = mimeMap[ext] ?? 'application/octet-stream';

        const putRes = await fetch(urlRes.upload_url, {
          method: 'POST',
          headers: { 'Content-Type': contentType },
          body: fileData,
        });
        if (!putRes.ok) {
          const text = await putRes.text().catch(() => 'unknown');
          return json({ ok: false, error: `Upload failed for ${filename}: ${putRes.status} ${text}` }, { status: 502 });
        }

        uploaded.push({ id: urlRes.file_id, title: filename });
      }

      const completePayload: Record<string, unknown> = {
        files: uploaded,
        channel_id: channel,
      };
      if (typeof body.thread_ts === 'string') completePayload.thread_ts = body.thread_ts;
      if (typeof body.initial_comment === 'string') completePayload.initial_comment = body.initial_comment;

      // files.completeUploadExternal requires JSON body, not form-urlencoded
      const completeRes = await fetch('https://slack.com/api/files.completeUploadExternal', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: JSON.stringify(completePayload),
      }).then(r => r.json()) as { ok: boolean; error?: string };
      if (!completeRes.ok) return json(completeRes, { status: 400 });

      return json({ ok: true, files: uploaded });
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/channels', 'GET', {
    description:
      'List channels. Default types=public_channel only (private_channel needs groups:read). full=true may spool huge JSON to `file`.',
    params: {
      types: {
        description:
          'Comma-separated: public_channel, private_channel, mpim, im (default public_channel)',
        type: 'string',
      },
      limit: { description: 'Max items (default 30, max 200)', type: 'string' },
      cursor: { description: 'Pagination cursor', type: 'string' },
      full: {
        description: 'If true, return full Slack channel objects (may spool to disk if huge)',
        type: 'string',
      },
    },
    annotations: { readOnlyHint: true },
  });
  router.get('/slack/channels', async (req) => {
    try {
      const token = requireToken();
      const types = qp(req, 'types') || 'public_channel';
      const limit = Math.min(
        200,
        Math.max(1, parseInt(qp(req, 'limit') ?? '30', 10) || 30),
      );
      const cursor = qp(req, 'cursor');
      const full = truthyFull(req);
      const ckey = channelCacheKey(types, limit, full, cursor);
      const hit = channelCache.get(ckey);
      if (hit && Date.now() - hit.ts < CHANNEL_CACHE_TTL) {
        return json(inlineOrSpool('get_slack_channels', hit.data));
      }
      const data = await slackApi<{
        ok: boolean;
        channels?: Record<string, unknown>[];
        response_metadata?: { next_cursor?: string };
      }>(token, 'conversations.list', {
        types,
        limit,
        ...(cursor ? { cursor } : {}),
      });
      if (!data.ok) return json(data, { status: 400 });
      const out = full ? data : slimChannels(data);
      channelCache.set(ckey, { data: out, ts: Date.now() });
      return json(inlineOrSpool('get_slack_channels', out));
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/history/:channel', 'GET', {
    description:
      'Fetch recent messages. Path param must be channel (C…/G…/D…), not channel_id.',
    params: {
      channel: { description: 'Conversation id C… G… or D…', type: 'string', required: true },
      limit: { description: 'Max messages (default 40, max 200)', type: 'string' },
      cursor: { description: 'Pagination cursor', type: 'string' },
      oldest: { description: 'Oldest message ts', type: 'string' },
      latest: { description: 'Latest message ts', type: 'string' },
      full: { description: 'Raw Slack message objects', type: 'string' },
    },
    annotations: { readOnlyHint: true },
  });
  router.get('/slack/history/:channel', async (req) => {
    try {
      const token = requireToken();
      const channel = req.params.channel as string;
      if (!channel) return json({ ok: false, error: 'channel required' }, { status: 400 });
      const limit = Math.min(
        200,
        Math.max(1, parseInt(qp(req, 'limit') ?? '40', 10) || 40),
      );
      const cursor = qp(req, 'cursor');
      const oldest = qp(req, 'oldest');
      const latest = qp(req, 'latest');
      const full = truthyFull(req);
      const historyParams = {
        channel,
        limit,
        ...(cursor ? { cursor } : {}),
        ...(oldest ? { oldest } : {}),
        ...(latest ? { latest } : {}),
      };
      let data = await slackApi<{
        ok: boolean;
        messages?: Record<string, unknown>[];
        has_more?: boolean;
        response_metadata?: { next_cursor?: string };
      }>(token, 'conversations.history', historyParams);
      // DM channels the bot isn't part of — retry with user token
      if (!data.ok && (data as any).error === 'channel_not_found') {
        const ut = optionalUserToken();
        if (ut) {
          data = await slackApi(ut, 'conversations.history', historyParams);
        }
      }
      if (!data.ok) return json(data, { status: 400 });
      const out = full ? data : slimHistory(data);
      return json(inlineOrSpool('get_slack_history_by_channel', out));
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/thread/:channel/:ts', 'GET', {
    description:
      'Fetch thread replies for a message. Use the parent message ts (from history or search results) to get all replies in the thread.',
    params: {
      channel: { description: 'Conversation id C… G… or D…', type: 'string', required: true },
      ts: { description: 'Thread parent message timestamp', type: 'string', required: true },
      limit: { description: 'Max replies (default 50, max 200)', type: 'string' },
      cursor: { description: 'Pagination cursor', type: 'string' },
      full: { description: 'Raw Slack message objects', type: 'string' },
    },
    annotations: { readOnlyHint: true },
  });
  router.get('/slack/thread/:channel/:ts', async (req) => {
    try {
      const token = requireToken();
      const channel = req.params.channel as string;
      const ts = req.params.ts as string;
      if (!channel) return json({ ok: false, error: 'channel required' }, { status: 400 });
      if (!ts) return json({ ok: false, error: 'ts required' }, { status: 400 });
      const limit = Math.min(200, Math.max(1, parseInt(qp(req, 'limit') ?? '50', 10) || 50));
      const cursor = qp(req, 'cursor');
      const full = truthyFull(req);
      const replyParams = { channel, ts, limit, ...(cursor ? { cursor } : {}) };
      let data = await slackApi<{
        ok: boolean;
        messages?: Record<string, unknown>[];
        has_more?: boolean;
        response_metadata?: { next_cursor?: string };
      }>(token, 'conversations.replies', replyParams);
      if (!data.ok && (data as any).error === 'channel_not_found') {
        const ut = optionalUserToken();
        if (ut) {
          data = await slackApi(ut, 'conversations.replies', replyParams);
        }
      }
      if (!data.ok) return json(data, { status: 400 });
      const out = full ? data : slimHistory(data);
      return json(inlineOrSpool('get_slack_thread', out));
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  base.describeMCP('/slack/search', 'GET', {
    description:
      'Workspace message search. Query param is q (not query). Requires SLACK_USER_TOKEN (xoxp-) — search.messages is user-token-only. Falls back to bot token but will likely return not_allowed_token_type.',
    params: {
      q: { description: 'Search query (Slack search syntax)', type: 'string', required: true },
      count: { description: 'Max results (default 15, max 100)', type: 'string' },
      page: { description: 'Page number', type: 'string' },
      sort: { description: 'timestamp | score', type: 'string' },
      sort_dir: { description: 'asc | desc', type: 'string' },
      full: { description: 'Raw Slack search matches', type: 'string' },
    },
    annotations: { readOnlyHint: true },
  });
  router.get('/slack/search', async (req) => {
    try {
      const token = optionalUserToken() ?? requireToken();
      const q = qp(req, 'q')?.trim();
      if (!q) return json({ ok: false, error: 'q is required' }, { status: 400 });
      const count = Math.min(
        100,
        Math.max(1, parseInt(qp(req, 'count') ?? '15', 10) || 15),
      );
      const page = Math.max(1, parseInt(qp(req, 'page') ?? '1', 10) || 1);
      const sort = qp(req, 'sort') || 'timestamp';
      const sort_dir = qp(req, 'sort_dir') || 'desc';
      const full = truthyFull(req);
      const data = await slackApi<{
        ok: boolean;
        messages?: { matches?: Record<string, unknown>[]; pagination?: unknown };
      }>(token, 'search.messages', {
        query: q,
        count,
        page,
        sort,
        sort_dir,
      });
      if (!data.ok) return json(data, { status: 400 });
      const out = full ? data : slimSearch(data);
      return json(inlineOrSpool('get_slack_search', out));
    } catch (e) {
      return json({ ok: false, error: errMessage(e) }, { status: 500 });
    }
  });

  registerDmUserRoutes(base);

  base.catchNotFound();

  const mcp = base.asMCP({
    name: 'mcp-slack',
    version: '0.1.0',
    instructions:
      'Slack: post_message needs real ids (C/G/D). DM flow: get_slack_users (search by name) or get_slack_user_by_email → get user ID (U…) → post_slack_dm_open → post_slack_message with D… channel_id. Prefer get_slack_users over by-email when you don\'t know the exact email. Thread replies: get_slack_thread with channel + parent ts. EDIT/DELETE: this app CAN edit and delete its own messages (whether posted as the bot or as the agent\'s user account — both tokens are tried automatically) — post_slack_update { channel, ts, text/blocks } and post_slack_delete { channel, ts }; the ts comes from post_slack_message\'s return. Prefer editing over posting a "Correction:" follow-up. Search (q param): requires SLACK_USER_TOKEN (xoxp-); auto-used when set. Invite bot to channels for is_member true. Large payloads: spooled file path. Full workflows: MCP resource skill://mcp-slack/workflow.',
  });

  augmentMcpWithSkillResource(mcp, {
    serverName: 'mcp-slack',
    repoRootAbs: resolve(import.meta.dirname, '..'),
    skillRelativePath: '.claude/skills/mcp-slack/SKILL.md',
  });

  // Keep the agent's Slack user account appearing online (cleared by mcp.close() for ephemeral embeds)
  let presencePingTimer: ReturnType<typeof setInterval> | undefined;
  const userToken = optionalUserToken();
  if (userToken) {
    const ping = () => slackApi(userToken, 'users.setPresence', { presence: 'auto' })
      .catch(e => console.error('[mcp-slack] setPresence failed:', e));
    ping();
    presencePingTimer = setInterval(ping, 5 * 60 * 1000);
  }

  Object.assign(mcp, {
    close: async (): Promise<void> => {
      if (presencePingTimer != null) {
        clearInterval(presencePingTimer);
        presencePingTimer = undefined;
      }
    },
  });

  async function httpFetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === '/health') {
      return json({ ok: true, service: 'mcp-slack' });
    }
    if (url.pathname === '/mcp' || url.pathname.startsWith('/mcp/')) {
      return mcp.httpHandler(req);
    }
    return base.fetchHandler(req);
  }

  return { mcp, httpFetch, base };
}
