/**
 * mcp-slack-use `ingress` — the inbound Slack protocol half for host apps (additive; new file).
 *
 * Owns everything transport-shaped: the `/api/slack/events` + `/api/slack/interactivity`
 * routes, rawBody + HMAC signature verification, url_verification, bot-echo skipping, dedupe,
 * DM identity resolution (which token owns the DM → useUserToken), file-stub hydration, a
 * per-thread FIFO queue, the hourglass reaction lifecycle, and SlackStreamer wiring.
 *
 * The host supplies the AGENT half via callbacks — zero Slack-transport code in the host:
 *   - shouldRespond(msg): the host's semantic gate (agent-channel rules, known threads, …).
 *   - onMessage(msg): returns AsyncIterable<AgentEvent> for one turn; the PACKAGE pipes it
 *     through its own SlackStreamer. The host does its own session/lock/persist side effects
 *     as it yields; it never touches the streamer.
 *   - onInteraction(payload): block_actions (poll votes / question submits).
 *   - onReplied(msg, replyTs): post-reply bookkeeping (thread alias, seen-ts) once the ts exists.
 *
 * Additive: does NOT modify the outbound MCP tool contract. New file.
 */
import { createHmac, timingSafeEqual } from 'node:crypto';
import { SlackStreamer } from './streamer.ts';
import { getBotUserId, getBotId, getAgentUserId, getFileInfo, canBotAccess, canAgentAccess, addReaction, removeReaction, postMessage } from './client.ts';

// Minimal structural Express types — avoids adding express as a dependency of this package.
interface ReqLike { headers: Record<string, unknown>; body: any; }
interface ResLike { status(code: number): ResLike; send(body?: unknown): unknown; json(body: unknown): unknown; }
interface AppLike { post(path: string, handler: (req: ReqLike, res: ResLike) => void | Promise<void>): void; }

/** One event yielded by a host turn. Only text_delta/tool_use drive the Slack stream; the rest
 *  are pass-through so the host can key its own side effects. Mirror of the host's stream shape. */
export type AgentEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'tool_use'; tool: string; toolUseId?: string; input?: unknown }
  | { type: 'tool_result'; toolUseId?: string; output?: string }
  | { type: 'tool_result_image'; dataUrl?: string }
  | { type: 'done'; stopReason?: string }
  | { type: 'error'; message: string }
  | { type: string; [k: string]: unknown };

export interface SlackRawEvent {
  type: string;
  channel: string;
  channel_type?: string;
  user?: string;
  text: string;
  ts: string;
  thread_ts?: string;
  bot_id?: string;
  subtype?: string;
  files?: { id?: string; url_private?: string; mimetype?: string; name?: string; size?: number }[];
}

/** Normalized inbound message handed to the host. `threadTs` is the resolved REPLY coordinate
 *  (for a top-level DM it is the message ts, so replies thread under it); `rawThreadTs` is the
 *  literal event.thread_ts (present only for actual thread replies). */
export interface IngressMessage {
  channel: string;
  channelType?: string;
  user?: string;
  text: string;
  ts: string;
  threadTs: string;
  rawThreadTs?: string;
  subtype?: string;
  botId?: string;
  type: string;
  files: any[];
  useUserToken: boolean;
  isDM: boolean;
  isMention: boolean;
  isThreadReply: boolean;
  textMentionsAgent: boolean;
  botUserId: string | null;
  agentUserId: string | null;
  raw: SlackRawEvent;
  /** Host-populated during onMessage; the SAME object is handed to onReplied so the host can
   *  carry per-turn state (e.g. its resolved session id) to its post-reply bookkeeping. */
  sessionId?: string;
  /** Package-populated before the turn is piped: close the streamed reply message in flight so
   *  anything the host posts itself (a question form, a file) lands ABOVE the rest of the reply
   *  instead of below it. See SlackStreamer.cut(). */
  cut?: () => Promise<void>;
}

export interface SlackIngressOptions {
  onMessage(msg: IngressMessage): AsyncIterable<AgentEvent> | null | Promise<AsyncIterable<AgentEvent> | null>;
  onInteraction(payload: any): void | Promise<void>;
  /** Host semantic gate (agent-channel rules, known/owned threads). Default: respond. */
  shouldRespond?(msg: IngressMessage): boolean | Promise<boolean>;
  /** Post-reply bookkeeping once the streamed reply's ts is known. */
  onReplied?(msg: IngressMessage, replyTs: string | null): void | Promise<void>;
  /** Applied to the final streamed text (e.g. resolve @mentions). Host-owned. */
  finalTransform?(text: string): Promise<string>;
  /** Markdown → Slack mrkdwn. Defaults to the package's mdToMrkdwn. */
  transform?(text: string): string;
  /** Called once identity ids are resolved at registration. */
  onReady?(ids: { botUserId: string | null; agentUserId: string | null; ownBotId: string | null }): void;
  eventsPath?: string;
  interactivityPath?: string;
  signingSecret?: string;
}

// ── Markdown → Slack mrkdwn ──────────────────────────────────────────────────
export function mdToMrkdwn(md: string): string {
  return md
    .replace(/^### (.+)$/gm, '*$1*')
    .replace(/^## (.+)$/gm, '*$1*')
    .replace(/^# (.+)$/gm, '*$1*')
    .replace(/\*\*(.+?)\*\*/g, '*$1*')
    .replace(/^---$/gm, '───')
    .replace(/\|(.+)\|\n\|[-| ]+\|\n/gm, '')
    .replace(/^\|(.+)\|$/gm, (_m, row) =>
      row.split('|').map((c: string) => c.trim()).filter(Boolean).join('  ·  '),
    )
    .replace(/```(\w*)\n([\s\S]*?)```/g, '```$2```')
    .replace(/`([^`]+)`/g, '`$1`');
}

/**
 * Pipe a host turn's AgentEvents through a SlackStreamer and return the reply ts.
 * Exported so a host recovery/resume path can reuse the exact same wiring without
 * importing SlackStreamer itself.
 */
export async function pipeAgentReply(
  opts: { channel: string; threadTs?: string; recipientUserId?: string; useUserToken?: boolean; transform?: (t: string) => string; finalTransform?: (t: string) => Promise<string>; handle?: { cut?: () => Promise<void> } },
  iter: AsyncIterable<AgentEvent>,
): Promise<string | null> {
  const streamer = new SlackStreamer({
    channel: opts.channel,
    threadTs: opts.threadTs,
    recipientUserId: opts.recipientUserId,
    useUserToken: opts.useUserToken,
    transform: opts.transform ?? mdToMrkdwn,
    finalTransform: opts.finalTransform,
  });
  // Hand the host a cut() before the first event: the iterator body only runs on the first next()
  // below, so anything it does mid-turn can already close the message in flight.
  if (opts.handle) opts.handle.cut = () => streamer.cut();
  try {
    for await (const ev of iter) {
      if (ev.type === 'text_delta' && typeof (ev as any).text === 'string') streamer.feed(ev as any);
      else if (ev.type === 'tool_use' && (ev as any).tool) streamer.feed(ev as any);
    }
  } catch (err) {
    streamer.abort();
    throw err;
  }
  return streamer.finish();
}

// ── Signature verification ────────────────────────────────────────────────────
function verifyRequest(req: ReqLike, signingSecret: string): boolean {
  const timestamp = req.headers['x-slack-request-timestamp'] as string;
  const signature = req.headers['x-slack-signature'] as string;
  if (!timestamp || !signature) return false;
  if (Math.abs(Date.now() / 1000 - Number(timestamp)) > 300) return false;
  const rawBody = (req as any).rawBody as Buffer;
  if (!rawBody) return false;
  const basestring = `v0:${timestamp}:${rawBody.toString()}`;
  const computed = 'v0=' + createHmac('sha256', signingSecret).update(basestring).digest('hex');
  try {
    return timingSafeEqual(Buffer.from(computed), Buffer.from(signature));
  } catch {
    return false;
  }
}

export function registerSlackIngress(app: AppLike, opts: SlackIngressOptions): void {
  const signingSecret = opts.signingSecret ?? process.env.SLACK_SIGNING_SECRET ?? '';
  const eventsPath = opts.eventsPath ?? '/api/slack/events';
  const interactivityPath = opts.interactivityPath ?? '/api/slack/interactivity';
  const transform = opts.transform ?? mdToMrkdwn;

  let agentUid: string | null = null;
  let botUid: string | null = null;
  let ownBotId: string | null = null;
  getAgentUserId().then(id => { agentUid = id; }).catch(err => console.warn('[mcp-slack-use] getAgentUserId failed:', err.message));
  getBotUserId().then(id => { botUid = id; }).catch(err => console.warn('[mcp-slack-use] getBotUserId failed:', err.message));
  getBotId().then(id => { ownBotId = id; console.log(`[mcp-slack-use] Own bot_id: ${id}`); opts.onReady?.({ botUserId: botUid, agentUserId: agentUid, ownBotId }); }).catch(err => console.warn('[mcp-slack-use] getBotId failed:', err.message));

  // ── DM identity cache ──────────────────────────────────────────────────────
  type DmIdentity = 'bot' | 'agent' | null;
  const dmIdentityCache = new Map<string, DmIdentity>();
  async function resolveDmIdentity(channel: string): Promise<DmIdentity> {
    const cached = dmIdentityCache.get(channel);
    if (cached !== undefined) return cached;
    const identity: DmIdentity = await canBotAccess(channel) ? 'bot'
      : await canAgentAccess(channel) ? 'agent'
      : null;
    dmIdentityCache.set(channel, identity);
    return identity;
  }

  // ── Per-thread FIFO queue ──────────────────────────────────────────────────
  const threadQueues = new Map<string, Promise<void>>();
  const seenEvents = new Set<string>();

  function threadQueueKey(msg: IngressMessage): string {
    const isDMRoot = msg.isDM && !msg.rawThreadTs;
    return isDMRoot ? `${msg.channel}:${msg.channel}` : `${msg.channel}:${msg.rawThreadTs || msg.ts}`;
  }

  function enqueue(msg: IngressMessage) {
    // Ack on ENQUEUE, not when the turn starts: a message queued behind a long-running turn
    // otherwise sits with zero feedback for minutes and the agent looks hung. The matching
    // removeReaction still fires when this message's own turn finishes.
    addReaction(msg.channel, msg.ts, 'hourglass_flowing_sand', msg.useUserToken).catch(err => console.warn('[mcp-slack-use] addReaction failed:', err.message));
    const key = threadQueueKey(msg);
    const prev = threadQueues.get(key) || Promise.resolve();
    const next = prev.then(() => runTurn(msg)).catch(err => console.error('[mcp-slack-use] ingress turn error:', err));
    threadQueues.set(key, next);
  }

  // ── One turn: stream, bookkeeping ──────────────────────────────────────────
  async function runTurn(msg: IngressMessage) {
    try {
      const iter = await opts.onMessage(msg);
      if (!iter) return;
      const replyTs = await pipeAgentReply(
        { channel: msg.channel, threadTs: msg.threadTs, recipientUserId: msg.user, useUserToken: msg.useUserToken, transform, finalTransform: opts.finalTransform, handle: msg },
        iter,
      );
      await opts.onReplied?.(msg, replyTs);
    } catch (err: any) {
      console.error('[mcp-slack-use] ingress turn failed:', err?.message || err);
      await postMessage(msg.channel, `⚠️ Error: ${err?.message}`, msg.threadTs, msg.useUserToken).catch(() => {});
    } finally {
      await removeReaction(msg.channel, msg.ts, 'hourglass_flowing_sand', msg.useUserToken).catch(err => console.warn('[mcp-slack-use] removeReaction failed:', err.message));
    }
  }

  // ── Events route ────────────────────────────────────────────────────────────
  app.post(eventsPath, async (req: ReqLike, res: ResLike) => {
    if (!verifyRequest(req, signingSecret)) return void res.status(401).send('Invalid signature');
    const body = req.body;
    if (body?.type === 'url_verification') return void res.json({ challenge: body.challenge });
    res.status(200).send();
    if (body?.type !== 'event_callback') return;

    const ev = body.event as SlackRawEvent;
    const auths = (body.authorizations || []).map((a: any) => `${a.is_bot ? 'bot' : 'user'}:${a.user_id}`).join(',');
    console.log(`[mcp-slack-use] Event: type=${ev.type} channel=${ev.channel} channel_type=${ev.channel_type} user=${ev.user} bot_id=${ev.bot_id || '-'} auths=[${auths}]`);

    // Hard protocol skips: bot echoes / the app's own posts.
    if (ev.subtype === 'bot_message') return;
    if (ev.bot_id) {
      if (!ownBotId || ev.bot_id === ownBotId) return;
      if (ev.user === botUid || (agentUid && ev.user === agentUid)) return;
    }
    if (agentUid && ev.user === agentUid && ev.channel_type !== 'im') return;

    const isDM = ev.channel_type === 'im';
    const isMention = ev.type === 'app_mention';
    const isThreadReply = !!(ev.thread_ts && ev.thread_ts !== ev.ts);
    const textMentionsAgent = !!(ev.text && (
      (botUid && ev.text.includes(`<@${botUid}>`)) ||
      (agentUid && ev.text.includes(`<@${agentUid}>`))
    ));
    const isDMRoot = isDM && !ev.thread_ts;
    const msg: IngressMessage = {
      channel: ev.channel,
      channelType: ev.channel_type,
      user: ev.user,
      text: ev.text || '',
      ts: ev.ts,
      threadTs: isDMRoot ? ev.ts : (ev.thread_ts || ev.ts),
      rawThreadTs: ev.thread_ts,
      subtype: ev.subtype,
      botId: ev.bot_id,
      type: ev.type,
      files: ev.files || [],
      useUserToken: false,
      isDM,
      isMention,
      isThreadReply,
      textMentionsAgent,
      botUserId: botUid,
      agentUserId: agentUid,
      raw: ev,
    };

    // Host semantic gate (agent-channel rules, known threads). Default: respond.
    if (opts.shouldRespond && !(await opts.shouldRespond(msg))) return;

    // Dedup AFTER the routing gate — Slack sends both `message` and `app_mention` for one ts;
    // deduping first would drop the `message` (rejected by the gate) and poison the set so the
    // real `app_mention` gets skipped.
    const dedupeKey = `${ev.channel}:${ev.ts}`;
    if (seenEvents.has(dedupeKey)) return;
    seenEvents.add(dedupeKey);
    setTimeout(() => seenEvents.delete(dedupeKey), 60_000);

    // DM dual-subscription: bot + user subs can fire for the same DM. Determine identity by who
    // the DM belongs to (cached). Drop DMs unrelated to either identity.
    const hasBotAuth = body.authorizations?.some((a: any) => a.is_bot);
    if (isDM && !hasBotAuth) {
      const identity = await resolveDmIdentity(ev.channel);
      if (identity === 'agent') msg.useUserToken = true;
      else if (identity !== 'bot') { console.log(`[mcp-slack-use] Skipping DM in ${ev.channel} (unrelated)`); return; }
    }

    // Hydrate file stubs (Events API delivers metadata-less stubs) before handing to the host.
    if (msg.files.length) {
      msg.files = await Promise.all(msg.files.map(async (f: any) =>
        (f.mimetype && f.url_private) ? f : ((await getFileInfo(f.id, msg.useUserToken)) ?? f),
      ));
    }

    enqueue(msg);
  });

  // ── Interactivity route (block_actions) ──────────────────────────────────────
  app.post(interactivityPath, async (req: ReqLike, res: ResLike) => {
    if (!verifyRequest(req, signingSecret)) return void res.status(401).send('Invalid signature');
    res.status(200).send();
    try {
      const payload = JSON.parse((req.body?.payload as string) ?? '{}');
      await opts.onInteraction(payload);
    } catch (err) {
      console.error('[mcp-slack-use] interactivity error:', (err as Error)?.message);
    }
  });

  console.log(`[mcp-slack-use] Ingress registered on ${eventsPath}, ${interactivityPath}`);
}
