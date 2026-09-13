// ============================================================
// BOT MANAGER — multi-account rotation with safe handover
//
// Rules this file enforces:
//   1. Exactly one bot is "active" (doing AFK work) at a time.
//   2. Every ROTATION_MS, a randomly chosen *different* account is
//      connected as a "candidate" while the active bot keeps running.
//   3. The candidate only becomes active AFTER it successfully spawns.
//   4. The previous active bot is only told to leave AFTER the
//      candidate has taken over.
//   5. If the active bot drops unexpectedly (kick/error/offline
//      server), a replacement is connected immediately and retried
//      forever (with backoff) until the server accepts one.
//   6. At most one "extra" connection attempt (candidate or
//      emergency replacement) is ever in flight at once, and every
//      timer created here is tracked and cleared so nothing leaks.
// ============================================================

const EventEmitter = require('events');
const mineflayer = require('mineflayer');

class BotManager extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.accounts = this._loadAccounts(config);

    const rotation = config.rotation || {};
    this.rotationMs = (rotation['interval-hours'] ? rotation['interval-hours'] * 60 * 60 * 1000 : null)
      || rotation['interval-ms']
      || 2 * 60 * 60 * 1000; // default: 2 hours
    this.minRetryMs = rotation['retry-min-ms'] || 3000;
    this.maxRetryMs = rotation['retry-max-ms'] || 30000;
    this.cooldownMs = rotation['cooldown-ms'] || 30000;
    this.spawnTimeoutMs = rotation['spawn-timeout-ms'] || 60000;

    this.autoReconnect = config.utils ? config.utils['auto-reconnect'] !== false : true;

    this.active = null;   // { bot, username, spawnedAt }
    this.pending = null;  // { bot, username, attemptId, inFlight, spawnTimeout, retryCount, role }
    this.handoverTimer = null;
    this.cooling = new Map(); // username -> timestamp when eligible again
    this._attemptSeq = 0;
    this._stopped = false;
  }

  // ---------------- account loading ----------------
  _loadAccounts(config) {
    const raw = Array.isArray(config['bot-accounts']) && config['bot-accounts'].length > 0
      ? config['bot-accounts']
      : [config['bot-account']];

    return raw.filter(Boolean).map((acc, idx) => {
      const n = idx + 1;
      // Optional per-slot environment overrides (same pattern this project
      // already uses for PORT / RENDER_EXTERNAL_URL) — settings.json values
      // are still the default and only source of truth if no env var is set.
      const username = process.env[`BOT_${n}_USERNAME`] || acc.username;
      const password = process.env[`BOT_${n}_PASSWORD`] || acc.password || '';
      const type = process.env[`BOT_${n}_AUTH`] || acc.type || 'offline';
      return { username, password, type };
    });
  }

  // ---------------- public API ----------------
  start() {
    if (this._stopped) return;
    this._connectAsActive([], 'initial');
  }

  // Used by index.js's uncaughtException handler: only starts a new
  // connection if nothing is active and nothing is already in flight,
  // so it never spawns a duplicate/racing connection after a crash.
  ensureActive() {
    if (this._stopped || !this.autoReconnect) return;
    if (this.active) return;
    if (this.pending && this.pending.inFlight) return;
    this._connectAsActive([], 'recovery');
  }

  // Force an out-of-band rotation right now (used by the "rotate" console command).
  forceHandover() {
    if (!this.active) return false;
    if (this.pending && this.pending.inFlight) return false;
    this._beginHandover();
    return true;
  }

  getStats() {
    return {
      activeBot: this.active ? this.active.username : null,
      pendingBot: this.pending ? this.pending.username : null,
      poolSize: this.accounts.length,
      nextRotationMs: this.handoverTimer ? this.rotationMs : null
    };
  }

  stop() {
    this._stopped = true;
    if (this.handoverTimer) { clearTimeout(this.handoverTimer); this.handoverTimer = null; }
    this._cancelPending();
    if (this.active && this.active.bot) {
      this._teardownBot(this.active.bot);
      this.active = null;
    }
  }

  // ---------------- account selection ----------------
  _availableAccounts(exclude) {
    const now = Date.now();
    return this.accounts.filter(a =>
      !exclude.includes(a.username) &&
      (!this.cooling.has(a.username) || this.cooling.get(a.username) <= now)
    );
  }

  _pickRandom(exclude = []) {
    let pool = this._availableAccounts(exclude);
    if (pool.length === 0) pool = this.accounts.filter(a => !exclude.includes(a.username));
    if (pool.length === 0) pool = this.accounts; // last resort (single-account setups)
    return pool[Math.floor(Math.random() * pool.length)];
  }

  // ---------------- connecting a (candidate / initial / replacement) bot ----------------
  _connectAsActive(exclude = [], role = 'initial') {
    if (this.pending && this.pending.inFlight) return; // one extra attempt at a time
    const account = this._pickRandom(exclude);
    this._beginPendingConnect(account, { role, retryCount: 0 });
  }

  _beginPendingConnect(account, meta) {
    this._cancelPending(); // safety net — invariant is "at most one pending attempt"

    const attemptId = ++this._attemptSeq;
    this.pending = { username: account.username, attemptId, inFlight: true, retryCount: meta.retryCount || 0, role: meta.role };

    this.emit('log', `[Rotation] Connecting ${account.username} (${meta.role}, attempt ${meta.retryCount + 1})...`);

    let bot;
    try {
      bot = mineflayer.createBot({
        username: account.username,
        password: account.password || undefined,
        auth: account.type || 'offline',
        host: this.config.server.ip,
        port: this.config.server.port,
        version: this.config.server.version,
        hideErrors: false,
        checkTimeoutInterval: 120000
      });
    } catch (e) {
      this.emit('log', `[Rotation] Failed to create bot for ${account.username}: ${e.message}`);
      this._retryPending(account, meta);
      return;
    }

    this.pending.bot = bot;

    const spawnTimeout = setTimeout(() => {
      if (!this._isCurrentPending(attemptId)) return;
      this.emit('log', `[Rotation] ${account.username} timed out before spawning`);
      this._teardownBot(bot);
      this._retryPending(account, meta);
    }, this.spawnTimeoutMs);
    this.pending.spawnTimeout = spawnTimeout;

    bot.once('spawn', () => {
      if (!this._isCurrentPending(attemptId)) return; // stale — a newer attempt superseded this one
      clearTimeout(spawnTimeout);
      this.pending.inFlight = false;
      this.emit('log', `[Rotation] ${account.username} successfully joined the server`);
      this._promote(account, bot, meta);
    });

    const onFailure = (label) => (reason) => {
      if (!this._isCurrentPending(attemptId)) return; // stale event from a superseded bot instance
      clearTimeout(spawnTimeout);
      this.emit('log', `[Rotation] ${account.username} failed to join (${label}): ${reason || ''}`);
      this._teardownBot(bot);
      this._retryPending(account, meta);
    };
    bot.once('end', onFailure('end'));
    bot.once('kicked', onFailure('kicked'));
    bot.on('error', (err) => {
      this.emit('log', `[Rotation] ${account.username} connection error: ${err.message}`);
      // 'end' normally follows a connection error; that handler drives the retry.
    });
  }

  _isCurrentPending(attemptId) {
    return !this._stopped && this.pending && this.pending.attemptId === attemptId;
  }

  _retryPending(account, meta) {
    if (this._stopped || !this.autoReconnect) { this.pending = null; return; }

    const retryCount = (meta.retryCount || 0) + 1;
    const delay = Math.min(this.minRetryMs + retryCount * 2000, this.maxRetryMs);
    this.cooling.set(account.username, Date.now() + this.cooldownMs);
    this.pending = null;

    this.emit('log', `[Rotation] Retrying in ${Math.round(delay / 1000)}s (this keeps happening automatically while the server is offline)`);
    this.emit('retry', retryCount);

    setTimeout(() => {
      if (this._stopped) return;
      if (this.pending && this.pending.inFlight) return; // something else already took this slot
      const exclude = this.active ? [this.active.username] : [];
      const nextAccount = this._pickRandom(exclude);
      this._beginPendingConnect(nextAccount, { role: meta.role, retryCount });
    }, delay);
  }

  _cancelPending() {
    if (!this.pending) return;
    if (this.pending.spawnTimeout) clearTimeout(this.pending.spawnTimeout);
    if (this.pending.bot) this._teardownBot(this.pending.bot);
    this.pending = null;
  }

  _teardownBot(bot) {
    try { bot.removeAllListeners(); } catch (e) { /* ignore */ }
    try { bot.end(); } catch (e) { /* ignore */ }
  }

  // ---------------- promotion / handover ----------------
  _promote(account, bot, meta) {
    const oldActive = this.active;
    this.active = { username: account.username, bot, spawnedAt: Date.now() };
    this.pending = null;

    // 1) New bot goes active FIRST.
    this.emit('active', bot, account.username, { previous: oldActive ? oldActive.username : null, role: meta.role });

    // 2) Only now does the old bot get told to leave.
    if (oldActive && oldActive.bot) {
      this.emit('log', `[Rotation] Handover complete — ${oldActive.username} is leaving, ${account.username} is now active`);
      this.emit('inactive', oldActive.bot, oldActive.username, { graceful: true, reason: 'handover' });
      this.cooling.set(oldActive.username, Date.now() + this.cooldownMs);
      this._teardownBot(oldActive.bot);
    }

    this._scheduleHandover();
    this._armActiveWatchdog(bot, account.username);
  }

  _armActiveWatchdog(bot, username) {
    const isStillThisActive = () => this.active && this.active.bot === bot;
    const onDown = (label) => (reason) => {
      if (!isStillThisActive()) return; // already superseded — nothing to do
      this.emit('log', `[Rotation] Active bot ${username} went down unexpectedly (${label}): ${reason || ''}`);
      this.active = null;
      if (this.handoverTimer) { clearTimeout(this.handoverTimer); this.handoverTimer = null; }
      this.emit('inactive', bot, username, { graceful: false, reason: label });
      this.cooling.set(username, Date.now() + this.cooldownMs);

      if (!this.autoReconnect) return;

      if (this.pending && this.pending.inFlight) {
        // A handover candidate was already on its way in — just let it land.
        this.emit('log', `[Rotation] A replacement (${this.pending.username}) is already connecting — will promote it as soon as it joins.`);
        return;
      }
      this._connectAsActive([username], 'replace');
    };
    bot.once('end', onDown('end'));
    bot.once('kicked', onDown('kicked'));
  }

  _scheduleHandover() {
    if (this.handoverTimer) clearTimeout(this.handoverTimer);
    if (!this.autoReconnect) return;
    this.handoverTimer = setTimeout(() => this._beginHandover(), this.rotationMs);
  }

  _beginHandover() {
    if (!this.active) return; // active already went down — the watchdog path owns replacement
    if (this.pending && this.pending.inFlight) return; // shouldn't happen, but stay safe
    const exclude = [this.active.username];
    const account = this._pickRandom(exclude);
    this.emit('log', `[Rotation] 2-hour timer elapsed for ${this.active.username} — bringing in ${account.username}`);
    this._beginPendingConnect(account, { role: 'handover', retryCount: 0 });
  }
}

module.exports = BotManager;
