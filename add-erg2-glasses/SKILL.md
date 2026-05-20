---
name: add-erg2-glasses
description: Add Even Realities G2 smart glasses channel. HTTP endpoint accepting OpenAI-format requests from the G2 companion app.
---

# Add Even Realities G2 Glasses Channel

Adds an HTTP endpoint that the G2 companion app connects to for AI agent interactions. The adapter handles synchronous request-response: it routes the user's voice-transcribed query through NanoClaw, waits for the agent's reply, and returns it in OpenAI chat completions format.

## Install

### Pre-flight (idempotent)

Skip to **Credentials** if all of these are already in place:

- `src/channels/erg2-glasses.ts` exists
- `src/channels/index.ts` contains `import './erg2-glasses.js';`

Otherwise continue. Every step below is safe to re-run.

### 1. Write the adapter file

If `src/channels/erg2-glasses.ts` does not exist, write the following content to it exactly:

```typescript
/**
 * Even Realities G2 smart glasses channel adapter.
 *
 * The G2 companion app sends HTTP POST requests in OpenAI chat completions
 * format. This adapter runs an HTTP server, routes inbound queries through
 * NanoClaw's pipeline, waits synchronously for the agent's response, and
 * returns it in OpenAI format.
 *
 * Key constraints:
 *   - Request-response only — no server-initiated push to the glasses
 *   - ~22s client timeout; we budget 20s
 *   - Plain text display (~500 chars, ~5 lines at the G2's default font)
 *   - One message per request; NanoClaw sessions provide conversation continuity
 *
 * Self-registers on import.
 */
import crypto from 'crypto';
import http from 'http';

import { readEnvFile } from '../env.js';
import { log } from '../log.js';
import type { ChannelAdapter, ChannelSetup, OutboundMessage } from './adapter.js';
import { registerChannelAdapter } from './channel-registry.js';

const RESPONSE_TIMEOUT_MS = 20_000;
const ACCUMULATION_WINDOW_MS = 1500;

interface PendingResponse {
  settle: (result: ResponseResult) => void;
  accumulateId: ReturnType<typeof setTimeout> | null;
  messages: string[];
  questionPayloads: QuestionPayload[];
}

interface ResponseResult {
  text: string;
  xNanoclaw?: Record<string, unknown>;
}

interface QuestionPayload {
  type: 'question';
  questionId: string;
  title: string;
  question: string;
  options: Array<{ label: string; value: string }>;
}

interface ActiveQuestion {
  questionId: string;
  options: Array<{ label: string; value: string }>;
}

function stripMarkdown(text: string): string {
  return text
    .replace(/```[\s\S]*?```/g, '[code]')
    .replace(/`(.+?)`/g, '$1')
    .replace(/^#{1,6}\s+/gm, '')
    .replace(/\*\*(.+?)\*\*/g, '$1')
    .replace(/__(.+?)__/g, '$1')
    .replace(/\*(.+?)\*/g, '$1')
    .replace(/_(.+?)_/g, '$1')
    .replace(/\[(.+?)\]\(.+?\)/g, '$1')
    .trim();
}

function createAdapter(): ChannelAdapter | null {
  const env = readEnvFile(['ERG2_GLASSES_TOKEN', 'ERG2_GLASSES_PORT', 'ERG2_GLASSES_HOST']);
  if (!env.ERG2_GLASSES_TOKEN) return null;

  const port = parseInt(env.ERG2_GLASSES_PORT || '7420', 10);
  const bindHost = env.ERG2_GLASSES_HOST || '0.0.0.0';
  const validTokens = new Set(
    env.ERG2_GLASSES_TOKEN.split(',')
      .map((t) => t.trim())
      .filter(Boolean),
  );

  let server: http.Server | null = null;
  let setupConfig: ChannelSetup | null = null;

  const pendingResponses = new Map<string, PendingResponse>();
  const pendingDeliveries = new Map<string, string[]>();
  const activeQuestions = new Map<string, ActiveQuestion>();

  function tokenToPlatformId(token: string): string {
    return `erg2-glasses:${crypto.createHash('sha256').update(token).digest('hex').slice(0, 16)}`;
  }

  function extractText(message: OutboundMessage): string | null {
    const c = message.content as Record<string, unknown> | string | undefined;
    if (typeof c === 'string') return stripMarkdown(c);
    if (!c || typeof c !== 'object') return null;

    if (c.type === 'ask_question') {
      const q = (c.question as string) || (c.title as string) || '';
      const opts = c.options as Array<{ label: string }> | undefined;
      if (opts?.length) {
        return `${q}\n\n${opts.map((o, i) => `${i + 1}. ${o.label}`).join('\n')}`;
      }
      return q;
    }

    if (typeof c.text === 'string') return stripMarkdown(c.text);
    if (typeof c.markdown === 'string') return stripMarkdown(c.markdown);
    if (c.operation === 'edit' && typeof c.text === 'string') return stripMarkdown(c.text);

    return null;
  }

  function extractQuestionPayload(message: OutboundMessage): QuestionPayload | null {
    const c = message.content as Record<string, unknown>;
    if (!c || typeof c !== 'object' || c.type !== 'ask_question') return null;
    return {
      type: 'question',
      questionId: c.questionId as string,
      title: c.title as string,
      question: c.question as string,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      options: ((c.options as any[]) || []).map((o: any) => ({
        label: String(o.label),
        value: String(o.value),
      })),
    };
  }

  function formatResponse(text: string, extras?: Record<string, unknown>): string {
    const body: Record<string, unknown> = {
      id: `chatcmpl-${crypto.randomUUID()}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: 'nanoclaw',
      choices: [
        {
          index: 0,
          message: { role: 'assistant', content: text },
          finish_reason: 'stop',
        },
      ],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
    if (extras) body.x_nanoclaw = extras;
    return JSON.stringify(body);
  }

  function tryMatchQuestionResponse(platformId: string, text: string): boolean {
    const q = activeQuestions.get(platformId);
    if (!q || !setupConfig) return false;

    const trimmed = text.trim();

    const num = parseInt(trimmed, 10);
    if (!isNaN(num) && num >= 1 && num <= q.options.length) {
      const selected = q.options[num - 1]!;
      activeQuestions.delete(platformId);
      try {
        setupConfig.onAction(q.questionId, selected.value, platformId);
      } catch (err) {
        log.error('ERG2 glasses: onAction threw', { err });
        return false;
      }
      return true;
    }

    const match = q.options.find(
      (o) =>
        o.label.toLowerCase() === trimmed.toLowerCase() ||
        o.value.toLowerCase() === trimmed.toLowerCase(),
    );
    if (match) {
      activeQuestions.delete(platformId);
      try {
        setupConfig.onAction(q.questionId, match.value, platformId);
      } catch (err) {
        log.error('ERG2 glasses: onAction threw', { err });
        return false;
      }
      return true;
    }

    return false;
  }

  function waitForDelivery(platformId: string): Promise<ResponseResult> {
    return new Promise<ResponseResult>((resolve) => {
      let settled = false;

      const settle = (result: ResponseResult) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeoutId);
        const p = pendingResponses.get(platformId);
        if (p?.accumulateId) clearTimeout(p.accumulateId);
        pendingResponses.delete(platformId);
        resolve(result);
      };

      const timeoutId = setTimeout(() => {
        const queued = pendingDeliveries.get(platformId);
        if (queued?.length) {
          pendingDeliveries.delete(platformId);
          settle({ text: queued.join('\n\n') });
        } else {
          settle({ text: 'Processing your request... try again shortly.' });
        }
      }, RESPONSE_TIMEOUT_MS);

      // Evict stale pending response (rapid re-request)
      const existing = pendingResponses.get(platformId);
      if (existing) {
        existing.settle({ text: 'New request received.' });
      }

      pendingResponses.set(platformId, {
        settle,
        accumulateId: null,
        messages: [],
        questionPayloads: [],
      });
    });
  }

  async function handleRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === 'GET' && req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', channel: 'erg2-glasses' }));
      return;
    }

    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Method not allowed' }));
      return;
    }

    const authHeader = req.headers.authorization;
    const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token || !validTokens.has(token)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    let body = '';
    for await (const chunk of req) {
      body += chunk;
      if (body.length > 1_000_000) {
        res.writeHead(413, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Payload too large' }));
        return;
      }
    }

    let payload: { model?: string; messages?: Array<{ role: string; content: string }> };
    try {
      payload = JSON.parse(body);
    } catch {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid JSON' }));
      return;
    }

    const userMsg = payload.messages?.filter((m) => m.role === 'user').pop();
    if (!userMsg?.content) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'No user message found' }));
      return;
    }

    const platformId = tokenToPlatformId(token);
    const text = userMsg.content.trim();

    log.info('ERG2 glasses request', { platformId, textLen: text.length });

    const isQuestionAnswer = tryMatchQuestionResponse(platformId, text);

    if (!isQuestionAnswer) {
      try {
        await setupConfig!.onInbound(platformId, null, {
          id: `erg2-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          kind: 'chat',
          timestamp: new Date().toISOString(),
          content: {
            text,
            sender: 'glasses',
            senderId: platformId,
          },
          isMention: true,
          isGroup: false,
        });
      } catch (err) {
        log.error('ERG2 glasses: inbound routing failed', { err });
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(formatResponse('Something went wrong. Please try again.'));
        return;
      }
    }

    const result = await waitForDelivery(platformId);

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(formatResponse(result.text, result.xNanoclaw));
  }

  const adapter: ChannelAdapter = {
    name: 'erg2-glasses',
    channelType: 'erg2-glasses',
    supportsThreads: false,

    async setup(config: ChannelSetup): Promise<void> {
      setupConfig = config;
      server = http.createServer((req, res) => {
        handleRequest(req, res).catch((err) => {
          log.error('ERG2 glasses: unhandled error', { err });
          if (!res.headersSent) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(formatResponse('Internal error'));
          }
        });
      });
      await new Promise<void>((resolve, reject) => {
        server!.once('error', reject);
        server!.listen(port, bindHost, () => {
          log.info('ERG2 glasses channel listening', { host: bindHost, port });
          resolve();
        });
      });
    },

    async teardown(): Promise<void> {
      for (const [, pending] of pendingResponses) {
        pending.settle({ text: 'Server shutting down.' });
      }
      pendingResponses.clear();

      if (server) {
        await new Promise<void>((resolve) => server!.close(() => resolve()));
        server = null;
      }
    },

    isConnected(): boolean {
      return server?.listening ?? false;
    },

    async deliver(
      platformId: string,
      _threadId: string | null,
      message: OutboundMessage,
    ): Promise<string | undefined> {
      const text = extractText(message);
      if (text === null) return undefined;

      const qp = extractQuestionPayload(message);
      if (qp) {
        activeQuestions.set(platformId, {
          questionId: qp.questionId,
          options: qp.options,
        });
      }

      const pending = pendingResponses.get(platformId);
      if (pending) {
        pending.messages.push(text);
        if (qp) pending.questionPayloads.push(qp);

        if (pending.accumulateId) clearTimeout(pending.accumulateId);
        pending.accumulateId = setTimeout(() => {
          const combined = pending.messages.join('\n\n');
          let xNanoclaw: Record<string, unknown> | undefined;
          if (pending.questionPayloads.length) {
            xNanoclaw = { questions: pending.questionPayloads };
          }

          const queued = pendingDeliveries.get(platformId);
          const finalText = queued?.length ? [...queued, combined].join('\n\n') : combined;
          if (queued?.length) pendingDeliveries.delete(platformId);

          pending.settle({ text: finalText, xNanoclaw });
        }, ACCUMULATION_WINDOW_MS);

        return undefined;
      }

      if (!pendingDeliveries.has(platformId)) {
        pendingDeliveries.set(platformId, []);
      }
      pendingDeliveries.get(platformId)!.push(text);
      log.info('ERG2 glasses: queued async delivery', {
        platformId,
        queueSize: pendingDeliveries.get(platformId)!.length,
      });

      return undefined;
    },
  };

  return adapter;
}

registerChannelAdapter('erg2-glasses', { factory: createAdapter });
```

### 2. Ensure the self-registration import

Check `src/channels/index.ts` contains:

```typescript
import './erg2-glasses.js';
```

Append if missing.

### 3. Build

```bash
pnpm run build
```

No additional npm packages required — the adapter uses Node's built-in `http` and `crypto` modules.

## Credentials

### Generate a bearer token

```bash
openssl rand -hex 32
```

### Configure environment

Add to `.env`:

```bash
ERG2_GLASSES_TOKEN=<your-token>
ERG2_GLASSES_PORT=7420
# ERG2_GLASSES_HOST=0.0.0.0  # optional, default binds all interfaces
```

**Multiple users**: comma-separate tokens:

```bash
ERG2_GLASSES_TOKEN=token_for_phil,token_for_other_user
```

Each token gets a unique platform identity (SHA-256 hash prefix), so each user gets their own session and messaging group.

Sync to container: `mkdir -p data/env && cp .env data/env/env`

### Network access

The G2 companion app sends requests from the phone over the internet. You need one of:

- **WireGuard / VPN**: Connect the phone to the same VPN — simplest, no TLS required
- **Reverse proxy**: Put the endpoint behind nginx/Caddy with TLS (recommended for internet exposure)
- **Tunnel**: Cloudflare Tunnel, ngrok, or similar

Even Hub requires HTTPS for custom agent URLs in production. WireGuard sidesteps this since traffic stays encrypted inside the tunnel.

### Configure the client plugin

Set the NanoClaw URL and bearer token in your Even Hub plugin's configuration. See `CLIENT.md` for the full plugin integration guide, protocol spec, and special commands convention.

For quick testing before the plugin is built, Even Hub's built-in Custom Agent feature (Settings > AI Agent > Custom Agent) accepts the same URL and token.

## Agent Group Setup

Add a note to the agent group's `CLAUDE.md` to optimize responses for the glasses display:

```markdown
## G2 Glasses Display

When responding to messages from the G2 smart glasses channel:
- Keep responses under 400 characters (5 lines on the HUD)
- Use plain text only — no markdown, URLs, or code blocks
- Be direct and concise — the user is glancing at a heads-up display
- For lists, use numbered items (1. 2. 3.) — the user can select by number
```

## Next Steps

If you're in the middle of `/setup`, return to the setup flow now.

Otherwise, run `/manage-channels` to wire this channel to an agent group.

## Channel Info

- **type**: `erg2-glasses`
- **platform-id-format**: SHA-256 hash prefix of the bearer token (16 hex chars) — determined automatically
- **how-to-find-id**: Check `logs/nanoclaw.log` for the `ERG2 glasses request` entry after a test request — it logs the `platformId`
- **supports-threads**: no
- **typical-use**: Voice-initiated Q&A via smart glasses HUD — short queries, concise answers
- **default-isolation**: Same agent group as your primary DM channel
- **push-capable**: no — agent-initiated messages are queued and delivered with the next user request
