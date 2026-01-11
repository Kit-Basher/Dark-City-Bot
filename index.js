const {
  Client,
  GatewayIntentBits,
  ActionRowBuilder,
  StringSelectMenuBuilder,
  PermissionsBitField,
  MessageFlags,
  REST,
  Routes,
  SlashCommandBuilder,
} = require('discord.js');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const DISCORD_BOT_TOKEN = requireEnv('DISCORD_BOT_TOKEN');
const DISCORD_APPLICATION_ID = requireEnv('DISCORD_APPLICATION_ID');
const DISCORD_GUILD_ID = requireEnv('DISCORD_GUILD_ID');

const MODERATOR_ROLE_ID = process.env.MODERATOR_ROLE_ID || process.env.DASHBOARD_ALLOWED_ROLE_ID || '';
const WRITER_ROLE_ID = process.env.WRITER_ROLE_ID || '';

const ASPECTS_CHANNEL_ID = process.env.ASPECTS_CHANNEL_ID || '1457635644338868317';

const DEFAULT_R_COOLDOWN_USER_MS = parseInt(process.env.R_COOLDOWN_USER_MS || '3000', 10);
const DEFAULT_R_COOLDOWN_CHANNEL_MS = parseInt(process.env.R_COOLDOWN_CHANNEL_MS || '1000', 10);

const DARK_CITY_API_BASE_URL = String(process.env.DARK_CITY_API_BASE_URL || '').trim().replace(/\/$/, '');
const DARK_CITY_MODERATOR_PASSWORD = String(process.env.DARK_CITY_MODERATOR_PASSWORD || '').trim();

let rCooldownUserMs = DEFAULT_R_COOLDOWN_USER_MS;
let rCooldownChannelMs = DEFAULT_R_COOLDOWN_CHANNEL_MS;

let inviteAutoDeleteEnabled = true;
let inviteWarnEnabled = true;
let inviteWarnDeleteSeconds = 12;

let lowTrustLinkFilterEnabled = true;
let lowTrustMinAccountAgeDays = 7;
let lowTrustWarnDmEnabled = true;

let spamAutoModEnabled = true;
let spamFloodWindowSeconds = 8;
let spamFloodMaxMessages = 5;
let spamRepeatWindowSeconds = 30;
let spamRepeatMaxRepeats = 3;
let spamWarnEnabled = true;
let spamWarnDeleteSeconds = 12;
let spamTimeoutEnabled = true;
let spamTimeoutMinutes = 10;
let spamStrikeDecayMinutes = 30;
/** @type {string[]} */
let spamIgnoredChannelIds = [];
/** @type {string[]} */
let spamBypassRoleIds = [];

let aspectsEnabled = true;
let aspectsMaxSelected = 2;

let xpEnabled = false;
let xpPerMessage = 1;
let xpCooldownSeconds = 60;
let xpMinMessageChars = 20;
/** @type {string[]} */
let xpAllowedChannelIds = [];

const BOT_HEARTBEAT_ENABLED = String(process.env.BOT_HEARTBEAT_ENABLED || 'true').trim().toLowerCase() !== 'false';
const BOT_HEARTBEAT_INTERVAL_SECONDS = parseInt(process.env.BOT_HEARTBEAT_INTERVAL_SECONDS || '30', 10);

const MONGODB_URI = process.env.MONGODB_URI;
let mongoClient;
let botDb;

// Reaction role configuration
const REACTION_ROLE_MESSAGE_ID = '1459463175370965194';
const READER_ROLE_ID = '1261096495860682873';

async function main() {

  if (process.env.NODE_ENV === 'production' && !MONGODB_URI) {
    console.log('ℹ️ Mongo: MONGODB_URI not set; bot settings/logs disabled');
    return;
  }

  mongoClient = new MongoClient(MONGODB_URI);
  await mongoClient.connect();
  botDb = mongoClient.db(process.env.BOT_DB_NAME || 'dark_city_bot');
  console.log('✅ Mongo: Connected');

  try {
    await botDb.collection('bot_heartbeats').createIndex({ guildId: 1, service: 1 }, { unique: true });
  } catch (e) {
    console.error('Mongo bot_heartbeats index failed:', e?.message || e);
  }
}

async function writeHeartbeat() {
  try {
    if (!botDb) return;
    const now = new Date();
    const instanceId = String(process.env.RENDER_INSTANCE_ID || process.env.HOSTNAME || '').trim() || null;
    const serviceId = String(process.env.RENDER_SERVICE_ID || '').trim() || null;
    await botDb.collection('bot_heartbeats').updateOne(
      { guildId: DISCORD_GUILD_ID, service: 'bot' },
      {
        $set: {
          guildId: DISCORD_GUILD_ID,
          service: 'bot',
          lastSeenAt: now,
          updatedAt: now,
          instanceId,
          serviceId,
        },
      },
      { upsert: true }
    );
  } catch (e) {
    console.error('Mongo heartbeat write failed:', e?.message || e);
  }
}

function startHeartbeatLoop() {
  if (!BOT_HEARTBEAT_ENABLED) return;
  if (!botDb) return;
  const intervalMs = Math.max(10, Number.isFinite(BOT_HEARTBEAT_INTERVAL_SECONDS) ? BOT_HEARTBEAT_INTERVAL_SECONDS : 30) * 1000;
  void writeHeartbeat();
  setInterval(() => {
    void writeHeartbeat();
  }, intervalMs);
}

async function pingUrl(url, timeoutMs) {
  const startedAt = Date.now();
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), Math.max(500, timeoutMs || 6000));
  try {
    const res = await fetch(url, { method: 'GET', redirect: 'follow', signal: ctrl.signal });
    return { ok: res.ok, status: res.status, ms: Date.now() - startedAt };
  } catch (e) {
    return { ok: false, status: 0, ms: Date.now() - startedAt, error: e?.name || e?.message || 'fetch_failed' };
  } finally {
    clearTimeout(t);
  }
}

async function getLatestHealthDoc(service) {
  try {
    if (!botDb) return null;
    return await botDb
      .collection('service_health_checks')
      .find({ guildId: DISCORD_GUILD_ID, service: String(service) })
      .sort({ checkedAt: -1 })
      .limit(1)
      .next();
  } catch {
    return null;
  }
}

async function getRecentHealthDocs(service, limit) {
  try {
    if (!botDb) return [];
    const n = Math.max(1, Math.min(200, Number.isFinite(limit) ? limit : 20));
    return await botDb
      .collection('service_health_checks')
      .find({ guildId: DISCORD_GUILD_ID, service: String(service) })
      .sort({ checkedAt: -1 })
      .limit(n)
      .toArray();
  } catch {
    return [];
  }
}

function computeErrorRate(rows) {
  if (!rows || rows.length === 0) return null;
  const failed = rows.filter((r) => r && r.ok === false).length;
  return failed / rows.length;
}

function computeFailStreak(rows) {
  if (!rows || rows.length === 0) return 0;
  let streak = 0;
  for (const r of rows) {
    if (r && r.ok === false) streak += 1;
    else break;
  }
  return streak;
}

function findMostRecentFailure(rows) {
  if (!rows || rows.length === 0) return null;
  for (const r of rows) {
    if (r && r.ok === false) return r;
  }
  return null;
}

function stripTrailingApi(baseUrl) {
  return String(baseUrl || '')
    .trim()
    .replace(/\/$/, '')
    .replace(/\/api$/, '');
}

function getConfiguredUrls() {
  const defaultApiBase = 'https://dark-city-3-0-reborn.onrender.com/api';
  const defaultMapBase = 'https://dark-city-map.onrender.com';
  const defaultDashBase = 'https://dark-city-bot-vd6t.onrender.com';

  const apiBase = stripTrailingApi(process.env.DARK_CITY_API_BASE_URL || defaultApiBase);
  const mapBase = String(process.env.DARK_CITY_MAP_BASE_URL || defaultMapBase).trim().replace(/\/$/, '');
  const dashBase = String(process.env.DARK_CITY_DASHBOARD_BASE_URL || defaultDashBase).trim().replace(/\/$/, '');
  return { apiBase, mapBase, dashBase };
}

function formatAgo(ms) {
  if (!Number.isFinite(ms) || ms < 0) return '?';
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.round(m / 60);
  return `${h}h`;
}

async function loadSettings() {
  if (!botDb) return;
  const doc = await botDb.collection('bot_settings').findOne({ guildId: DISCORD_GUILD_ID });
  if (!doc) return;

  if (Number.isFinite(doc.rCooldownUserMs)) rCooldownUserMs = doc.rCooldownUserMs;
  if (Number.isFinite(doc.rCooldownChannelMs)) rCooldownChannelMs = doc.rCooldownChannelMs;

  if (typeof doc.inviteAutoDeleteEnabled === 'boolean') inviteAutoDeleteEnabled = doc.inviteAutoDeleteEnabled;
  if (typeof doc.inviteWarnEnabled === 'boolean') inviteWarnEnabled = doc.inviteWarnEnabled;
  if (Number.isFinite(doc.inviteWarnDeleteSeconds)) inviteWarnDeleteSeconds = doc.inviteWarnDeleteSeconds;

  if (typeof doc.lowTrustLinkFilterEnabled === 'boolean') lowTrustLinkFilterEnabled = doc.lowTrustLinkFilterEnabled;
  if (Number.isFinite(doc.lowTrustMinAccountAgeDays)) lowTrustMinAccountAgeDays = doc.lowTrustMinAccountAgeDays;
  if (typeof doc.lowTrustWarnDmEnabled === 'boolean') lowTrustWarnDmEnabled = doc.lowTrustWarnDmEnabled;

  if (typeof doc.spamAutoModEnabled === 'boolean') spamAutoModEnabled = doc.spamAutoModEnabled;
  if (Number.isFinite(doc.spamFloodWindowSeconds)) spamFloodWindowSeconds = doc.spamFloodWindowSeconds;
  if (Number.isFinite(doc.spamFloodMaxMessages)) spamFloodMaxMessages = doc.spamFloodMaxMessages;
  if (Number.isFinite(doc.spamRepeatWindowSeconds)) spamRepeatWindowSeconds = doc.spamRepeatWindowSeconds;
  if (Number.isFinite(doc.spamRepeatMaxRepeats)) spamRepeatMaxRepeats = doc.spamRepeatMaxRepeats;
  if (typeof doc.spamWarnEnabled === 'boolean') spamWarnEnabled = doc.spamWarnEnabled;
  if (Number.isFinite(doc.spamWarnDeleteSeconds)) spamWarnDeleteSeconds = doc.spamWarnDeleteSeconds;
  if (typeof doc.spamTimeoutEnabled === 'boolean') spamTimeoutEnabled = doc.spamTimeoutEnabled;
  if (Number.isFinite(doc.spamTimeoutMinutes)) spamTimeoutMinutes = doc.spamTimeoutMinutes;
  if (Number.isFinite(doc.spamStrikeDecayMinutes)) spamStrikeDecayMinutes = doc.spamStrikeDecayMinutes;

  if (Array.isArray(doc.spamIgnoredChannelIds)) {
    spamIgnoredChannelIds = doc.spamIgnoredChannelIds.map((x) => String(x).trim()).filter(Boolean);
  } else if (typeof doc.spamIgnoredChannelIds === 'string') {
    spamIgnoredChannelIds = doc.spamIgnoredChannelIds
      .split(/[\n,]/g)
      .map((x) => String(x).trim())
      .filter(Boolean);
  }

  if (Array.isArray(doc.spamBypassRoleIds)) {
    spamBypassRoleIds = doc.spamBypassRoleIds.map((x) => String(x).trim()).filter(Boolean);
  } else if (typeof doc.spamBypassRoleIds === 'string') {
    spamBypassRoleIds = doc.spamBypassRoleIds
      .split(/[\n,]/g)
      .map((x) => String(x).trim())
      .filter(Boolean);
  }

  if (typeof doc.aspectsEnabled === 'boolean') aspectsEnabled = doc.aspectsEnabled;
  if (Number.isFinite(doc.aspectsMaxSelected)) aspectsMaxSelected = doc.aspectsMaxSelected;

  if (typeof doc.xpEnabled === 'boolean') xpEnabled = doc.xpEnabled;
  if (Number.isFinite(doc.xpPerMessage)) xpPerMessage = doc.xpPerMessage;
  if (Number.isFinite(doc.xpCooldownSeconds)) xpCooldownSeconds = doc.xpCooldownSeconds;
  if (Number.isFinite(doc.xpMinMessageChars)) xpMinMessageChars = doc.xpMinMessageChars;
  if (Array.isArray(doc.xpAllowedChannelIds)) {
    xpAllowedChannelIds = doc.xpAllowedChannelIds.map((x) => String(x).trim()).filter(Boolean);
  } else if (typeof doc.xpAllowedChannelIds === 'string') {
    xpAllowedChannelIds = doc.xpAllowedChannelIds
      .split(/[\n,]/g)
      .map((x) => String(x).trim())
      .filter(Boolean);
  }
}

async function ensureSettingsDoc() {
  if (!botDb) return;
  await botDb.collection('bot_settings').updateOne(
    { guildId: DISCORD_GUILD_ID },
    {
      $setOnInsert: {
        guildId: DISCORD_GUILD_ID,
        rCooldownUserMs: DEFAULT_R_COOLDOWN_USER_MS,
        rCooldownChannelMs: DEFAULT_R_COOLDOWN_CHANNEL_MS,
        inviteAutoDeleteEnabled: true,
        inviteWarnEnabled: true,
        inviteWarnDeleteSeconds: 12,
        lowTrustLinkFilterEnabled: true,
        lowTrustMinAccountAgeDays: 7,
        lowTrustWarnDmEnabled: true,

        spamAutoModEnabled: true,
        spamFloodWindowSeconds: 8,
        spamFloodMaxMessages: 5,
        spamRepeatWindowSeconds: 30,
        spamRepeatMaxRepeats: 3,
        spamWarnEnabled: true,
        spamWarnDeleteSeconds: 12,
        spamTimeoutEnabled: true,
        spamTimeoutMinutes: 10,
        spamStrikeDecayMinutes: 30,
        spamIgnoredChannelIds: [],
        spamBypassRoleIds: [],
        aspectsEnabled: true,
        aspectsMaxSelected: 2,

        xpEnabled: false,
        xpPerMessage: 1,
        xpCooldownSeconds: 60,
        xpMinMessageChars: 20,
        xpAllowedChannelIds: [],
        createdAt: new Date(),
      },
    },
    { upsert: true }
  );
}

async function logEvent(level, event, message, meta) {
  try {
    if (!botDb) return;
    await botDb.collection('bot_logs').insertOne({
      guildId: DISCORD_GUILD_ID,
      level,
      event,
      message,
      meta: meta || null,
      createdAt: new Date(),
    });
  } catch (e) {
    console.error('Mongo logEvent failed:', e);
  }
}

const rollCommand = new SlashCommandBuilder()
  .setName('r')
  .setDescription('Roll 2d6');

const rskillCommand = new SlashCommandBuilder()
  .setName('rskill')
  .setDescription('Roll 2d6 with character skill bonus')
  .addStringOption((opt) =>
    opt
      .setName('skill')
      .setDescription('Skill name to add bonus from')
      .setRequired(true)
  );

const startCommand = new SlashCommandBuilder()
  .setName('start')
  .setDescription('Start a scene and add it to the calendar')
  .addStringOption((opt) =>
    opt
      .setName('players')
      .setDescription('Players involved (mention them with @)')
      .setRequired(true)
  )
  .addStringOption((opt) =>
    opt
      .setName('date')
      .setDescription('Date of the scene (dd/mm/yyyy, defaults to today)')
      .setRequired(false)
  );

const endCommand = new SlashCommandBuilder()
  .setName('end')
  .setDescription('End the current scene');

const purgeCommand = new SlashCommandBuilder()
  .setName('purge')
  .setDescription('Delete recent messages in this channel (mods only)')
  .addIntegerOption((opt) =>
    opt
      .setName('count')
      .setDescription('How many messages to delete (1-100)')
      .setRequired(true)
      .setMinValue(1)
      .setMaxValue(100)
  );

const timeoutCommand = new SlashCommandBuilder()
  .setName('timeout')
  .setDescription('Timeout a member (mods only)')
  .addUserOption((opt) => opt.setName('user').setDescription('User to timeout').setRequired(true))
  .addIntegerOption((opt) =>
    opt
      .setName('minutes')
      .setDescription('Duration in minutes (1-10080)')
      .setRequired(true)
      .setMinValue(1)
      .setMaxValue(10080)
  )
  .addStringOption((opt) => opt.setName('reason').setDescription('Reason (optional)').setRequired(false));

const untimeoutCommand = new SlashCommandBuilder()
  .setName('untimeout')
  .setDescription('Remove timeout from a member (mods only)')
  .addUserOption((opt) => opt.setName('user').setDescription('User to untimeout').setRequired(true))
  .addStringOption((opt) => opt.setName('reason').setDescription('Reason (optional)').setRequired(false));

const slowmodeCommand = new SlashCommandBuilder()
  .setName('slowmode')
  .setDescription('Set channel slowmode (mods only)')
  .addIntegerOption((opt) =>
    opt
      .setName('seconds')
      .setDescription('Slowmode seconds (0-21600)')
      .setRequired(true)
      .setMinValue(0)
      .setMaxValue(21600)
  );

const lockCommand = new SlashCommandBuilder()
  .setName('lock')
  .setDescription('Lock this channel for @everyone (mods only)')
  .addStringOption((opt) => opt.setName('reason').setDescription('Reason (optional)').setRequired(false));

const unlockCommand = new SlashCommandBuilder()
  .setName('unlock')
  .setDescription('Unlock this channel for @everyone (mods only)')
  .addStringOption((opt) => opt.setName('reason').setDescription('Reason (optional)').setRequired(false));

const aspectsPostCommand = new SlashCommandBuilder()
  .setName('aspects_post')
  .setDescription('Post/update the Aspects role menus in the #aspects channel (mods only)');

const aspectsMissingCommand = new SlashCommandBuilder()
  .setName('aspects_missing')
  .setDescription('List missing Aspect role names (for manual creation) (mods only)');

const aspectsCleanupPrefixedCommand = new SlashCommandBuilder()
  .setName('aspects_cleanup_prefixed')
  .setDescription('Delete unused legacy Aspect: roles (memberCount=0). Use before reposting without prefix (mods only)');

const uokCommand = new SlashCommandBuilder()
  .setName('uok')
  .setDescription('Quick bot liveness check');

const statusReportCommand = new SlashCommandBuilder()
  .setName('statusreport')
  .setDescription('Brief status report for game/map/dashboard/bot (writers and mods only)');

const fullReportCommand = new SlashCommandBuilder()
  .setName('fullreport')
  .setDescription('Expanded report for game/map/dashboard/bot (writers and mods only)');

const cardCommand = new SlashCommandBuilder()
  .setName('card')
  .setDescription('Show linked character card (level/xp) (writers and mods only)');

const linkCharacterCommand = new SlashCommandBuilder()
  .setName('linkcharacter')
  .setDescription('Link Discord account to approved character (writers and mods only)');

const awardXpCommand = new SlashCommandBuilder()
  .setName('awardxp')
  .setDescription('Award XP to a member\'s linked character (writers and mods only)')
  .addUserOption((opt) => opt.setName('user').setDescription('User to award XP to').setRequired(true))
  .addIntegerOption((opt) =>
    opt
      .setName('amount')
      .setDescription('XP amount (can be negative)')
      .setRequired(true)
      .setMinValue(-100000)
      .setMaxValue(100000)
  );

const totalFpCommand = new SlashCommandBuilder()
  .setName('totalfp')
  .setDescription('Show current fate points (writers and mods only)');

const readerRoleCommand = new SlashCommandBuilder()
  .setName('reader')
  .setDescription('Get the reader role for accessing server content');

const useFpCommand = new SlashCommandBuilder()
  .setName('fp')
  .setDescription('Use 1 fate point (writers and mods only)');

const addFpCommand = new SlashCommandBuilder()
  .setName('fpup')
  .setDescription('Add 1 fate point (writers and mods only)')
  .addUserOption((opt) => opt.setName('user').setDescription('User to add fate point to (defaults to yourself)').setRequired(false));

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(DISCORD_BOT_TOKEN);
  await rest.put(Routes.applicationGuildCommands(DISCORD_APPLICATION_ID, DISCORD_GUILD_ID), {
    body: [
      rollCommand.toJSON(),
      rskillCommand.toJSON(),
      startCommand.toJSON(),
      endCommand.toJSON(),
      uokCommand.toJSON(),
      statusReportCommand.toJSON(),
      fullReportCommand.toJSON(),
      purgeCommand.toJSON(),
      timeoutCommand.toJSON(),
      untimeoutCommand.toJSON(),
      slowmodeCommand.toJSON(),
      lockCommand.toJSON(),
      unlockCommand.toJSON(),
      aspectsPostCommand.toJSON(),
      aspectsMissingCommand.toJSON(),
      aspectsCleanupPrefixedCommand.toJSON(),
      cardCommand.toJSON(),
      linkCharacterCommand.toJSON(),
      awardXpCommand.toJSON(),
      totalFpCommand.toJSON(),
      useFpCommand.toJSON(),
      addFpCommand.toJSON(),
      readerRoleCommand.toJSON(),
    ],
  });
}

function getNicknameCharacterName(member) {
  const raw = String(member?.displayName || member?.nickname || '').trim();
  if (!raw) return '';
  const idx = raw.indexOf('(');
  const name = (idx >= 0 ? raw.slice(0, idx) : raw).trim();
  return name;
}

function assertGameApiConfigured() {
  if (!DARK_CITY_API_BASE_URL) {
    throw new Error('DARK_CITY_API_BASE_URL is not set');
  }
}

async function darkCityApiRequest(path, opts) {
  assertGameApiConfigured();
  const url = `${DARK_CITY_API_BASE_URL}${path}`;
  const headers = {
    'Content-Type': 'application/json',
    ...(opts?.headers || {}),
  };
  const res = await fetch(url, { ...opts, headers });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!res.ok) {
    const msg = json?.error || json?.message || text || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

async function darkCityApiGetPublicJson(path) {
  if (!DARK_CITY_API_BASE_URL) {
    throw new Error('DARK_CITY_API_BASE_URL is not set');
  }
  const url = `${DARK_CITY_API_BASE_URL}${path}`;
  const res = await fetch(url);
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!res.ok) {
    const msg = json?.error || json?.message || text || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

async function getMissingAspectRoleNames(guild, categories) {
  if (!guild) return [];

  const rolesFetchTimeoutMs = parseInt(process.env.ASPECTS_ROLES_FETCH_TIMEOUT_MS || '45000', 10);
  await withTimeout(guild.roles.fetch(), rolesFetchTimeoutMs, 'guild.roles.fetch').catch(() => null);

  const existingByName = new Set();
  for (const role of guild.roles.cache.values()) {
    if (role?.name) existingByName.add(role.name);
  }

  /** @type {string[]} */
  const missing = [];
  for (const c of categories) {
    for (const it of c.items) {
      const roleName = buildAspectRoleName(it.name);
      if (existingByName.has(roleName) || existingByName.has(buildLegacyAspectRoleName(it.name))) continue;
      missing.push(roleName);
    }
  }

  return missing;
}

async function cleanupPrefixedAspectRoles(guild) {
  if (!guild?.members) throw new Error('Guild not available');
  const me = guild.members.me || (await guild.members.fetchMe().catch(() => null));
  if (!me) throw new Error('Could not resolve bot member');

  if (!me.permissions.has(PermissionsBitField.Flags.ManageRoles)) {
    throw new Error('Missing Manage Roles permission');
  }

  const roles = guild.roles.cache
    .filter((r) => r?.name?.startsWith('Aspect: '))
    .map((r) => r);

  let deleted = 0;
  let skippedInUse = 0;
  let failed = 0;

  for (const role of roles) {
    if (!role) continue;
    if (role.managed) {
      failed += 1;
      continue;
    }

    // memberCount is maintained by Discord for roles in guild.
    if ((role.members?.size || 0) > 0) {
      skippedInUse += 1;
      continue;
    }

    try {
      await role.delete('Dark City bot: removing legacy Aspect: role prefix');
      deleted += 1;
    } catch (e) {
      failed += 1;
    }
  }

  return { deleted, skippedInUse, failed, total: roles.length };
}

function readAspectsFromMarkdown() {
  const filePath = path.join(__dirname, 'dark_city_aspects.md');
  const raw = fs.readFileSync(filePath, 'utf8');

  /** @type {{ key: string, label: string, items: { name: string }[] }[]} */
  const categories = [];
  let current = null;

  for (const line of raw.split(/\r?\n/)) {
    const heading = line.match(/^##\s+(.+?)\s*(?:\(\d+\))?\s*$/);
    if (heading) {
      const label = heading[1].trim();
      const key = label.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
      current = { key, label, items: [] };
      categories.push(current);
      continue;
    }

    const item = line.match(/^\d+\.\s+\*\*(.+?)\*\*/);
    if (item && current) {
      const name = item[1].trim();
      if (name) current.items.push({ name });
    }
  }

  const allNames = new Set();
  for (const c of categories) {
    c.items = c.items.filter((it) => {
      const k = it.name.toLowerCase();
      if (allNames.has(k)) return false;
      allNames.add(k);
      return true;
    });
  }

  return categories;
}

function buildAspectRoleName(aspectName) {
  return `${aspectName}`;
}

function buildLegacyAspectRoleName(aspectName) {
  return `Aspect: ${aspectName}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)),
  ]);
}

async function ensureAspectRoles(guild, categories) {
  const created = [];
  const failed = [];
  let remaining = 0;
  if (!guild?.members) return { created, failed, remaining: 0, missing: [] };
  const me = guild.members.me || (await guild.members.fetchMe().catch(() => null));
  if (!me) return { created, failed, remaining: 0, missing: [] };
  const canManageRoles = me.permissions.has(PermissionsBitField.Flags.ManageRoles);
  if (!canManageRoles) return { created, failed, remaining: 0, missing: [] };

  // Ensure role cache is fully populated; partial caches can cause us to think roles are missing.
  // NOTE: Timeout does not cancel the underlying request, but the /aspects_post mutex prevents piling up runs.
  const rolesFetchTimeoutMs = parseInt(process.env.ASPECTS_ROLES_FETCH_TIMEOUT_MS || '45000', 10);
  await withTimeout(guild.roles.fetch(), rolesFetchTimeoutMs, 'guild.roles.fetch').catch(() => null);

  const roleCache = guild.roles.cache;
  const existingByName = new Map();
  for (const role of roleCache.values()) {
    existingByName.set(role.name, role);
  }

  const maxCreatesPerRun = parseInt(process.env.ASPECTS_ROLE_CREATES_PER_RUN || '25', 10);
  const createDelayMs = parseInt(process.env.ASPECTS_ROLE_CREATE_DELAY_MS || '350', 10);

  // Build list of missing roles up front so we can compute remaining reliably.
  /** @type {string[]} */
  const missingRoleNames = [];
  for (const c of categories) {
    for (const it of c.items) {
      const roleName = buildAspectRoleName(it.name);
      if (existingByName.has(roleName) || existingByName.has(buildLegacyAspectRoleName(it.name))) continue;
      missingRoleNames.push(roleName);
    }
  }

  remaining = missingRoleNames.length;
  const toCreateNow = missingRoleNames.slice(0, Math.max(0, maxCreatesPerRun));
  let attemptedCreates = 0;
  let consecutiveFailures = 0;
  const maxConsecutiveFailures = parseInt(process.env.ASPECTS_ROLE_MAX_CONSECUTIVE_FAILURES || '3', 10);
  const roleCreateTimeoutMs = parseInt(process.env.ASPECTS_ROLE_CREATE_TIMEOUT_MS || '45000', 10);

  for (const roleName of toCreateNow) {
    try {
      attemptedCreates += 1;
      consecutiveFailures = 0;

      if (attemptedCreates % 5 === 1) {
        logEvent('info', 'aspect_role_create_progress', 'Aspect role create progress', {
          attemptedCreates,
          created: created.length,
          failed: failed.length,
          nextRoleName: roleName,
          maxCreatesPerRun,
          remainingAtStart: missingRoleNames.length,
        });
      }

      const createdRole = await withTimeout(
        guild.roles.create({
          name: roleName,
          mentionable: false,
          hoist: false,
          reason: 'Dark City bot: creating missing Aspect role',
        }),
        roleCreateTimeoutMs,
        'guild.roles.create'
      );

      existingByName.set(roleName, createdRole);
      created.push(createdRole.id);
      remaining -= 1;

      if (createDelayMs > 0) await sleep(createDelayMs);
    } catch (e) {
      failed.push(roleName);
      console.error('Failed to create aspect role:', roleName, e);
      logEvent('error', 'aspect_role_create_failed', 'Failed to create aspect role', {
        roleName,
        discordCode: e?.code,
        status: e?.status,
        message: e?.message || String(e),
      });

      consecutiveFailures += 1;
      // Back off a bit on timeouts / transient Discord stalls.
      await sleep(2000);

      // If Discord is stalling repeatedly, abort this batch so /aspects_post can update its reply.
      if (consecutiveFailures >= maxConsecutiveFailures) {
        break;
      }

      // Otherwise, keep going so one bad/stalled role doesn't block all progress.
      continue;
    }
  }

  // remaining should include:
  // - any not attempted this run (missingRoleNames.length - toCreateNow.length)
  // - any attempted but not created due to failure/abort (implicitly included since we break early)
  remaining = Math.max(0, missingRoleNames.length - created.length);

  return { created, failed, remaining, missing: missingRoleNames };
}

async function getAspectRoleMaps(guild, categories) {
  const roleNameToId = new Map();
  const roleIdToCategoryKey = new Map();
  const allAspectRoleIds = new Set();

  for (const c of categories) {
    for (const it of c.items) {
      const roleName = buildAspectRoleName(it.name);
      const role =
        guild.roles.cache.find((r) => r.name === roleName) ||
        guild.roles.cache.find((r) => r.name === buildLegacyAspectRoleName(it.name));
      if (!role) continue;
      roleNameToId.set(it.name, role.id);
      roleIdToCategoryKey.set(role.id, c.key);
      allAspectRoleIds.add(role.id);
    }
  }

  return { roleNameToId, roleIdToCategoryKey, allAspectRoleIds };
}

async function assertCanPostInAspectsChannel(guild) {
  const channel = await guild.channels.fetch(ASPECTS_CHANNEL_ID).catch(() => null);
  if (!channel || !channel.isTextBased()) {
    throw new Error(`Aspects channel not found or not text-based: ${ASPECTS_CHANNEL_ID}`);
  }

  const me = guild.members.me || (await guild.members.fetchMe().catch(() => null));
  if (me && channel?.permissionsFor) {
    const perms = channel.permissionsFor(me);
    const missing = [];
    if (perms && !perms.has(PermissionsBitField.Flags.ViewChannel)) missing.push('View Channel');
    if (perms && !perms.has(PermissionsBitField.Flags.SendMessages)) missing.push('Send Messages');
    if (perms && !perms.has(PermissionsBitField.Flags.ReadMessageHistory)) missing.push('Read Message History');
    if (missing.length > 0) {
      throw new Error(
        `Missing channel permissions in #aspects (channelId=${ASPECTS_CHANNEL_ID}): ${missing.join(', ')}`
      );
    }
  }

  return channel;
}

async function postAspectsMenus(guild, categories) {
  const channel = await assertCanPostInAspectsChannel(guild);

  // Ensure the role cache is populated so we can map all aspect names to role IDs.
  const rolesFetchTimeoutMs = parseInt(process.env.ASPECTS_ROLES_FETCH_TIMEOUT_MS || '45000', 10);
  await withTimeout(guild.roles.fetch(), rolesFetchTimeoutMs, 'guild.roles.fetch').catch(() => null);

  const { roleNameToId } = await getAspectRoleMaps(guild, categories);

  await channel.send('Select up to **2** Aspects total across all categories. You can change them anytime.');

  for (const c of categories) {
    const options = c.items
      .map((it) => {
        const roleId = roleNameToId.get(it.name);
        if (!roleId) return null;
        return {
          label: it.name,
          value: roleId,
        };
      })
      .filter(Boolean);

    for (let i = 0; i < options.length; i += 25) {
      const chunk = options.slice(i, i + 25);
      const menu = new StringSelectMenuBuilder()
        .setCustomId(`aspects:${c.key}:${i / 25}`)
        .setPlaceholder(`${c.label} (pick up to 2 total)`)
        .setMinValues(0)
        .setMaxValues(Math.min(25, aspectsMaxSelected))
        .addOptions(chunk);

      const row = new ActionRowBuilder().addComponents(menu);
      await channel.send({ components: [row] });
    }
  }
}

function hasModPermission(member) {
  if (!member) return false;

  try {
    if (MODERATOR_ROLE_ID) {
      const roles = member.roles;
      if (roles?.cache?.has?.(MODERATOR_ROLE_ID)) return true;
      if (Array.isArray(roles) && roles.includes(MODERATOR_ROLE_ID)) return true;
      return false;
    }
  } catch {
    // ignore
  }

  const perms = member.permissions;
  if (!perms) return false;

  return (
    perms.has(PermissionsBitField.Flags.Administrator) ||
    perms.has(PermissionsBitField.Flags.ManageGuild) ||
    perms.has(PermissionsBitField.Flags.ManageMessages) ||
    perms.has(PermissionsBitField.Flags.ModerateMembers)
  );
}

function hasWriterOrModPermission(member) {
  if (!member) return false;

  try {
    const roles = member.roles;
    
    // Check for writer role
    if (WRITER_ROLE_ID) {
      if (roles?.cache?.has?.(WRITER_ROLE_ID)) return true;
      if (Array.isArray(roles) && roles.includes(WRITER_ROLE_ID)) return true;
    }
    
    // Check for moderator role
    if (MODERATOR_ROLE_ID) {
      if (roles?.cache?.has?.(MODERATOR_ROLE_ID)) return true;
      if (Array.isArray(roles) && roles.includes(MODERATOR_ROLE_ID)) return true;
    }
    
    // Check for Discord permissions
    const perms = member.permissions;
    if (perms) {
      return (
        perms.has(PermissionsBitField.Flags.Administrator) ||
        perms.has(PermissionsBitField.Flags.ManageGuild) ||
        perms.has(PermissionsBitField.Flags.ManageMessages) ||
        perms.has(PermissionsBitField.Flags.ModerateMembers)
      );
    }
  } catch {
    // ignore
  }

  return false;
}

async function requireModerator(interaction) {
  const member = interaction.member;
  const allowed = hasModPermission(member);
  if (allowed) return true;
  await interaction.reply({ content: 'Access denied (mods only).', flags: MessageFlags.Ephemeral });
  return false;
}

async function requireWriterOrMod(interaction) {
  const member = interaction.member;
  const allowed = hasWriterOrModPermission(member);
  if (allowed) return true;
  await interaction.reply({ content: 'Access denied (writers and mods only).', flags: MessageFlags.Ephemeral });
  return false;
}

function roll2d6() {
  const d1 = Math.floor(Math.random() * 6) + 1;
  const d2 = Math.floor(Math.random() * 6) + 1;
  return { d1, d2, total: d1 + d2 };
}

const lastRollByUser = new Map();
const lastRollByChannel = new Map();

// Scene tracking
const activeScenesByChannel = new Map(); // channelId -> scene data
const sceneMessagesByChannel = new Map(); // channelId -> { startMessageId, calendarMessageId }

const lastInviteWarnByUser = new Map();
const lastLowTrustDmWarnByUser = new Map();
const lastSpamWarnByUser = new Map();

const lastXpAwardAtByUser = new Map();

const recentMessageTimestampsByUser = new Map();
const lastMessageNormByUser = new Map();
// Value shape: { count: number, lastAt: number }
const spamStrikeCountByUser = new Map();

// Prevent concurrent runs of long-running commands per guild.
// Value shape: { startedAt: number, timer: NodeJS.Timeout }
const aspectsPostLocks = new Map();

function clearAspectsPostLock(guildId) {
  const lock = aspectsPostLocks.get(guildId);
  if (lock?.timer) {
    clearTimeout(lock.timer);
  }
  aspectsPostLocks.delete(guildId);
}

const INVITE_REGEX = /(?:https?:\/\/)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com\/invite)\/[A-Za-z0-9-]+/i;
const URL_REGEX = /https?:\/\//i;

function pruneWarnEntries(now) {
  pruneOldEntries(lastInviteWarnByUser, 10 * 60_000, now);
  pruneOldEntries(lastLowTrustDmWarnByUser, 10 * 60_000, now);
  pruneOldEntries(lastSpamWarnByUser, 10 * 60_000, now);
}

function isXpAllowedChannel(message) {
  const ch = message?.channel;
  if (!ch) return false;
  if (!xpAllowedChannelIds || xpAllowedChannelIds.length === 0) return false;

  const channelId = ch?.id;
  const parentId = ch?.parentId;
  if (channelId && xpAllowedChannelIds.includes(channelId)) return true;
  if (parentId && xpAllowedChannelIds.includes(parentId)) return true;
  return false;
}

function normalizeForRepeatCheck(content) {
  return String(content || '')
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, ' ')
    .replace(/<@!?\d+>/g, '@user')
    .replace(/<#[0-9]+>/g, '#channel')
    .replace(/<@&[0-9]+>/g, '@role')
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function isLowTrustAccount(user, now) {
  if (!user?.createdTimestamp) return false;
  const minMs = Math.max(0, lowTrustMinAccountAgeDays) * 24 * 60 * 60 * 1000;
  return (now - user.createdTimestamp) < minMs;
}

function getCooldownRemainingMs(map, key, cooldownMs, now) {
  if (!key || cooldownMs <= 0) return 0;
  const last = map.get(key) || 0;
  const remaining = (last + cooldownMs) - now;
  return remaining > 0 ? remaining : 0;
}

function pruneOldEntries(map, olderThanMs, now) {
  if (olderThanMs <= 0) return;
  for (const [key, ts] of map.entries()) {
    if (!ts || (now - ts) > olderThanMs) map.delete(key);
  }
}

async function main() {
  await ensureSettingsDoc();
  await loadSettings();

  startHeartbeatLoop();

  if (botDb) {
    setInterval(() => {
      loadSettings().catch(() => {});
    }, 30_000);
  }

  await registerCommands();

  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMembers,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent,
      GatewayIntentBits.GuildMessageReactions,
    ],
  });

  client.once('clientReady', () => {
    console.log(`🤖 Logged in as ${client.user.tag}`);
    console.log(`🔍 Reaction role configured for message: ${REACTION_ROLE_MESSAGE_ID}`);
    console.log(`👑 Reader role ID: ${READER_ROLE_ID}`);
    console.log(`🏰 Guild ID: ${DISCORD_GUILD_ID}`);
    logEvent('info', 'bot_ready', 'Bot logged in', {
      userTag: client.user.tag,
      rCooldownUserMs,
      rCooldownChannelMs,
    });
  });

  client.on('interactionCreate', async (interaction) => {
    try {
      if (!interaction.isChatInputCommand()) return;

      // Helper functions for scene management
      function parseDate(dateString) {
        if (!dateString) return new Date(); // Default to today
        
        const parts = dateString.split('/');
        if (parts.length !== 3) return null;
        
        const [day, month, year] = parts.map(p => parseInt(p, 10));
        if (!Number.isFinite(day) || !Number.isFinite(month) || !Number.isFinite(year)) return null;
        
        // Create date in local timezone
        const date = new Date(year, month - 1, day);
        
        // Validate the date is valid
        if (date.getDate() !== day || date.getMonth() !== month - 1 || date.getFullYear() !== year) {
          return null;
        }
        
        return date;
      }

      function extractUserIds(mentionsString) {
        const userIds = [];
        const mentionPattern = /<@!?(\d+)>/g;
        let match;
        while ((match = mentionPattern.exec(mentionsString)) !== null) {
          userIds.push(match[1]);
        }
        return userIds;
      }

      async function getCharacterNames(userIds) {
        const characterNames = [];
        
        for (const userId of userIds) {
          try {
            const characterUrl = `${DARK_CITY_API_BASE_URL}/api/characters/discord/by-user/${userId}`;
            const characterResponse = await fetch(characterUrl, {
              method: 'GET',
              headers: { 'Content-Type': 'application/json' },
              timeout: 3000,
            });

            if (characterResponse.ok) {
              const character = await characterResponse.json();
              characterNames.push(character.name);
            } else {
              // Try to get user from Discord API as fallback
              const user = await client.users.fetch(userId).catch(() => null);
              characterNames.push(user?.username || `User-${userId.slice(-4)}`);
            }
          } catch (error) {
            console.error(`Error fetching character for user ${userId}:`, error);
            characterNames.push(`User-${userId.slice(-4)}`);
          }
        }
        
        return characterNames;
      }

      async function createCalendarPost(characterNames, date, startMessageUrl) {
        // This would integrate with your calendar system
        // For now, we'll simulate it with a placeholder
        const calendarMessage = `📅 **Scene Scheduled**\n**Date:** ${date.toLocaleDateString('en-GB')}\n**Players:** ${characterNames.join(', ')}\n**Link:** [View Scene](${startMessageUrl})\n\n*Status: Active*`;
        
        // In a real implementation, this would post to your calendar channel/system
        // For now, return a mock calendar message ID
        return { calendarMessageId: 'mock-' + Date.now(), calendarMessage };
      }

      if (interaction.commandName === 'uok') {
        const uptimeSec = Math.floor(process.uptime());
        const pingMs = Number.isFinite(client.ws?.ping) ? Math.round(client.ws.ping) : null;
        const pingPart = pingMs === null ? '' : ` | ping ${pingMs}ms`;
        await interaction.reply({ content: `✅ OK | uptime ${uptimeSec}s${pingPart}`, flags: MessageFlags.Ephemeral });
        return;
      }

      if (interaction.commandName === 'statusreport') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });

        const { apiBase, mapBase, dashBase } = getConfiguredUrls();

        const targets = [
          { label: 'Game', service: 'game', url: apiBase ? `${apiBase}/status-ping` : null },
          { label: 'Map', service: 'map', url: mapBase ? `${mapBase}/status-ping` : null },
          { label: 'Dashboard', service: 'dashboard', url: dashBase ? `${dashBase}/health` : null },
        ];

        const lines = [];
        lines.push('**Dark City Status Report**');

        for (const t of targets) {
          if (!t.url) {
            lines.push(`- **${t.label}**: missing URL env var`);
            continue;
          }

          const [live, last] = await Promise.all([
            pingUrl(t.url, 6000),
            getLatestHealthDoc(t.service),
          ]);

          const lastOk = last ? (last.ok ? 'OK' : 'FAIL') : 'n/a';
          const lastAge = last?.checkedAt ? formatAgo(Date.now() - new Date(last.checkedAt).getTime()) : 'n/a';
          const lastDetail = last ? `${lastOk} ${lastAge} ago` : 'n/a';

          const liveDetail = live.ok
            ? `OK (${live.status}) ${live.ms}ms`
            : `FAIL (${live.status || 0}${live.error ? ` ${live.error}` : ''}) ${live.ms}ms`;

          lines.push(`- **${t.label}**: live ${liveDetail} | last ${lastDetail}`);
        }

        // Bot service (this process)
        const uptimeSec = Math.floor(process.uptime());
        const wsPingMs = Number.isFinite(client.ws?.ping) ? Math.round(client.ws.ping) : null;
        const wsPingPart = wsPingMs === null ? 'n/a' : `${wsPingMs}ms`;

        let hbAge = null;
        if (botDb) {
          const hb = await botDb.collection('bot_heartbeats').findOne({ guildId: DISCORD_GUILD_ID, service: 'bot' });
          if (hb?.lastSeenAt) hbAge = Date.now() - new Date(hb.lastSeenAt).getTime();
        }
        const hbPart = hbAge === null ? 'n/a' : `${formatAgo(hbAge)} ago`;
        lines.push(`- **Bot service**: uptime ${uptimeSec}s | ws ping ${wsPingPart} | heartbeat ${hbPart}`);

        await interaction.editReply(lines.join('\n'));
        return;
      }

      if (interaction.commandName === 'fullreport') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });

        const { apiBase, mapBase, dashBase } = getConfiguredUrls();

        const liveTargets = [
          { label: 'Game', service: 'game', url: apiBase ? `${apiBase}/status-ping` : null, kind: 'status-ping' },
          { label: 'Map', service: 'map', url: mapBase ? `${mapBase}/status-ping` : null, kind: 'status-ping' },
          { label: 'Dashboard', service: 'dashboard', url: dashBase ? `${dashBase}/health` : null, kind: 'health' },
        ];

        const lines = [];
        lines.push('**Dark City Full Report**');

        for (const t of liveTargets) {
          if (!t.url) {
            lines.push(`- **${t.label}**: missing URL`);
            continue;
          }

          const [live, last, recent] = await Promise.all([
            pingUrl(t.url, 6000),
            getLatestHealthDoc(t.service),
            getRecentHealthDocs(t.service, 20),
          ]);

          const rate = computeErrorRate(recent);
          const ratePart = rate === null ? 'n/a' : `${Math.round(rate * 100)}% (${recent.length})`;
          const streak = computeFailStreak(recent);
          const fail = findMostRecentFailure(recent);
          const failAge = fail?.checkedAt ? formatAgo(Date.now() - new Date(fail.checkedAt).getTime()) : null;
          const failPart = fail
            ? ` | lastFail ${failAge} ago (${fail.status || 0}${fail.error ? ` ${fail.error}` : ''})`
            : '';

          const lastOk = last ? (last.ok ? 'OK' : 'FAIL') : 'n/a';
          const lastAge = last?.checkedAt ? formatAgo(Date.now() - new Date(last.checkedAt).getTime()) : 'n/a';
          const lastMeta = last
            ? `${lastOk} ${lastAge} ago` +
              `${last.ok ? '' : ` | last=${last.status || 0}${last.error ? ` (${last.error})` : ''}`}`
            : 'n/a';

          const liveMeta = live.ok
            ? `OK (${live.status}) ${live.ms}ms`
            : `FAIL (${live.status || 0}${live.error ? ` ${live.error}` : ''}) ${live.ms}ms`;

          lines.push(
            `- **${t.label}**: live ${liveMeta} | last ${lastMeta} | err ${ratePart} | streak ${streak}${failPart}`
          );
        }

        // Bot service (this process)
        const uptimeSec = Math.floor(process.uptime());
        const wsPingMs = Number.isFinite(client.ws?.ping) ? Math.round(client.ws.ping) : null;
        const wsPingPart = wsPingMs === null ? 'n/a' : `${wsPingMs}ms`;

        let hbAge = null;
        let hbExtra = '';
        if (botDb) {
          const hb = await botDb.collection('bot_heartbeats').findOne({ guildId: DISCORD_GUILD_ID, service: 'bot' });
          if (hb?.lastSeenAt) hbAge = Date.now() - new Date(hb.lastSeenAt).getTime();
          if (hb?.serviceId) hbExtra += ` | serviceId ${hb.serviceId}`;
          if (hb?.instanceId) hbExtra += ` | instance ${hb.instanceId}`;
        }
        const hbPart = hbAge === null ? 'n/a' : `${formatAgo(hbAge)} ago`;
        lines.push(`- **Bot service**: uptime ${uptimeSec}s | ws ping ${wsPingPart} | heartbeat ${hbPart}${hbExtra}`);

        // Keep under Discord limits
        const out = lines.join('\n').slice(0, 1900);
        await interaction.editReply(out);
        return;
      }

      if (interaction.commandName === 'aspects_post') {
        if (!interaction.guild) {
          await interaction.reply({ content: 'Must be used in a server.', flags: MessageFlags.Ephemeral });
          return;
        }

        // Defer immediately to avoid Discord's 3-second interaction acknowledgement window.
        try {
          await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        } catch (e) {
          // Interaction might be expired/invalid; nothing else we can do.
          console.error('aspects_post deferReply error:', e);
          return;
        }

        const guildId = interaction.guild.id;
        const existingLock = aspectsPostLocks.get(guildId);
        if (existingLock) {
          // If something went wrong previously, don't block forever.
          const ageMs = Date.now() - (existingLock.startedAt || 0);
          if (ageMs > 10 * 60_000) {
            clearAspectsPostLock(guildId);
          } else {
            await interaction.editReply(
              'An /aspects_post run is already in progress for this server. Please wait a minute and try again.'
            );
            return;
          }
        }

        const ttlMs = parseInt(process.env.ASPECTS_POST_LOCK_TTL_MS || '600000', 10);
        const timer = setTimeout(() => {
          clearAspectsPostLock(guildId);
        }, ttlMs);
        aspectsPostLocks.set(guildId, { startedAt: Date.now(), timer });

        if (!(await requireWriterOrMod(interaction))) {
          clearAspectsPostLock(guildId);
          await interaction.editReply('Invalid moderator password.');
          return;
        }

        logEvent('info', 'aspects_post_started', 'Aspects post started', {
          userId: interaction.user?.id,
          channelId: ASPECTS_CHANNEL_ID,
        });

        if (!aspectsEnabled) {
          await interaction.editReply('Aspects system is disabled.');
          return;
        }

        try {
          await assertCanPostInAspectsChannel(interaction.guild);
          const categories = readAspectsFromMarkdown();
          await interaction.editReply('Creating/updating Aspect roles (batched; run may need repeating)...');
          const { created, failed, remaining, missing } = await ensureAspectRoles(interaction.guild, categories);

          console.log('🔧 aspects_post role-create result:', {
            created: created?.length || 0,
            failed: failed?.length || 0,
            remaining: remaining || 0,
          });
          logEvent('info', 'aspects_post_role_create_result', 'Aspects role creation result', {
            userId: interaction.user?.id,
            created: created?.length || 0,
            failed: failed?.length || 0,
            remaining: remaining || 0,
          });

          if (failed && failed.length > 0) {
            const sample = failed.slice(0, 5).join(', ');
            await interaction.editReply(
              `Role creation finished. Created ${created.length} missing roles. Failed ${failed.length} (sample: ${sample}). Remaining: ${remaining || 0}.`
            );
          } else {
            await interaction.editReply(
              `Role creation finished. Created ${created.length} missing roles. Remaining: ${remaining || 0}.`
            );
          }

          if (remaining && remaining > 0) {
            const remainingNote = `Created ${created.length} roles this run. Remaining roles to create: ${remaining}. Re-run /aspects_post to continue. (When remaining reaches 0, it will post the menus.)`;
            const missingNames = Array.isArray(missing) ? missing : [];
            if (missingNames.length > 0 && remaining <= 15) {
              const list = missingNames.slice(0, 50).join('\n');
              await interaction.editReply(`${remainingNote}\n\nMissing role names (you can create these manually):\n\`\`\`\n${list}\n\`\`\``);
            } else {
              await interaction.editReply(remainingNote);
            }
            return;
          }

          await interaction.editReply('All roles are present. Posting menus...');
          await postAspectsMenus(interaction.guild, categories);

          logEvent('info', 'aspects_posted', 'Posted Aspects menus', {
            userId: interaction.user?.id,
            channelId: ASPECTS_CHANNEL_ID,
            createdRoles: created.length,
            failedRoles: failed?.length || 0,
          });

          if (failed && failed.length > 0) {
            const sample = failed.slice(0, 10).join(', ');
            await interaction.editReply(
              `Posted Aspects menus in <#${ASPECTS_CHANNEL_ID}>. Created ${created.length} missing roles. Failed to create ${failed.length} roles (sample: ${sample}). Check logs for the Discord error code.`
            );
          } else {
            await interaction.editReply(`Posted Aspects menus in <#${ASPECTS_CHANNEL_ID}>. Created ${created.length} missing roles.`);
          }
          return;
        } catch (e) {
          console.error('aspects_post error:', e);
          logEvent('error', 'aspects_post_error', e?.message || String(e), { stack: e?.stack });
          // We already deferred; always use editReply here.
          await interaction.editReply(
            `Failed to run /aspects_post. Error: ${e?.message || String(e)}`
          );
          return;
        } finally {
          clearAspectsPostLock(interaction.guild.id);
        }
      }

      if (interaction.commandName === 'aspects_missing') {
        if (!interaction.guild) {
          await interaction.reply({ content: 'Must be used in a server.', flags: MessageFlags.Ephemeral });
          return;
        }

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });

        if (!(await requireWriterOrMod(interaction))) {
          await interaction.editReply('Invalid moderator password.');
          return;
        }

        try {
          const categories = readAspectsFromMarkdown();
          const missing = await getMissingAspectRoleNames(interaction.guild, categories);

          if (!missing || missing.length === 0) {
            await interaction.editReply('No missing Aspect roles detected.');
            return;
          }

          const header = `Missing Aspect roles: ${missing.length}. Create these roles manually (exact spelling):`;
          const list = missing.slice(0, 80).join('\n');
          const truncated = missing.length > 80 ? `\n\n(Showing first 80 of ${missing.length})` : '';
          await interaction.editReply(`${header}\n\n\`\`\`\n${list}\n\`\`\`${truncated}`);
          return;
        } catch (e) {
          console.error('aspects_missing error:', e);
          logEvent('error', 'aspects_missing_error', e?.message || String(e), { stack: e?.stack });
          await interaction.editReply(`Failed to list missing roles. Error: ${e?.message || String(e)}`);
          return;
        }
      }

      if (interaction.commandName === 'aspects_cleanup_prefixed') {
        if (!(await requireModerator(interaction))) return;
        if (!interaction.guild) {
          await interaction.reply({ content: 'Must be used in a server.', flags: MessageFlags.Ephemeral });
          return;
        }
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });

        try {
          const result = await cleanupPrefixedAspectRoles(interaction.guild);
          logEvent('info', 'aspects_cleanup_prefixed', 'Cleaned up prefixed aspect roles', {
            userId: interaction.user?.id,
            ...result,
          });
          await interaction.editReply(
            `Cleanup complete. Deleted ${result.deleted}/${result.total} roles. Skipped in-use: ${result.skippedInUse}. Failed: ${result.failed}.`
          );
          return;
        } catch (e) {
          console.error('aspects_cleanup_prefixed error:', e);
          logEvent('error', 'aspects_cleanup_prefixed_error', e?.message || String(e), { stack: e?.stack });
          await interaction.editReply(
            `Cleanup failed. Ensure the bot has Manage Roles and that its role is above the Aspect roles. Error: ${e?.message || String(e)}`
          );
          return;
        }
      }

      if (interaction.commandName === 'card') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const discordUserId = interaction.user?.id;
          if (!discordUserId) {
            await interaction.editReply('Could not resolve your Discord user ID.');
            return;
          }
          const data = await darkCityApiGetPublicJson(`/api/characters/discord/by-user/${discordUserId}`);
          const c = data?.character;
          const next = data?.next;
          if (!c) {
            await interaction.editReply('No character data returned.');
            return;
          }

          const nextLine = next?.nextLevel
            ? `Next: level ${next.nextLevel} at ${next.nextLevelXp} XP (${next.remaining} more)`
            : 'Max level reached.';
          await interaction.editReply(
            `**${c.name}**\nLevel: **${c.level}**\nXP: **${c.xp}**\n${nextLine}`
          );
          return;
        } catch (e) {
          if (e?.status === 404) {
            const name = getNicknameCharacterName(interaction.member);
            await interaction.editReply(
              `No linked approved character found for you.\n` +
                `Set your nickname to \`Character Name (Player Name)\` and run \`/linkcharacter\`.\n` +
                (name ? `I currently read your character name as: **${name}**` : 'I could not read a character name from your nickname.')
            );
            return;
          }
          await interaction.editReply(`Failed to fetch card: ${e?.message || String(e)}`);
          return;
        }
      }

      if (interaction.commandName === 'totalfp') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const discordUserId = interaction.user?.id;
          if (!discordUserId) {
            await interaction.editReply('Could not resolve your Discord user ID.');
            return;
          }

          const result = await darkCityApiRequest(`/api/characters/discord/by-user/${discordUserId}`);
          if (!result || !result.character) {
            await interaction.editReply('No approved character found linked to your account.');
            return;
          }

          const fatePoints = result.character.fatePoints || 0;
          const characterName = result.character.name || 'Unknown';
          await interaction.editReply(`**${characterName}** has **${fatePoints}** fate points.`);
        } catch (e) {
          await interaction.editReply(`Failed to fetch fate points: ${e?.message || String(e)}`);
        }
        return;
      }

      if (interaction.commandName === 'fp') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const discordUserId = interaction.user?.id;
          if (!discordUserId) {
            await interaction.editReply('Could not resolve your Discord user ID.');
            return;
          }

          const result = await darkCityApiRequest(`/api/characters/discord/by-user/${discordUserId}`);
          if (!result || !result.character) {
            await interaction.editReply('No approved character found linked to your account.');
            return;
          }

          const currentFp = result.character.fatePoints || 0;
          if (currentFp <= 0) {
            await interaction.editReply('You have no fate points remaining!');
            return;
          }

          // Update fate points by calling the character update API
          const updateResult = await darkCityApiRequest('/api/characters/discord/update-fate-points', {
            method: 'POST',
            body: JSON.stringify({ 
              discordUserId: discordUserId, 
              fatePoints: currentFp - 1 
            }),
          });

          if (updateResult && updateResult.success) {
            await interaction.editReply(`Used 1 fate point. You now have **${currentFp - 1}** fate points remaining.`);
            logEvent('info', 'fate_point_used', 'User spent 1 fate point', {
              userId: discordUserId,
              previousFp: currentFp,
              newFp: currentFp - 1
            });
          } else {
            await interaction.editReply('Failed to update fate points. Please try again.');
          }
        } catch (e) {
          await interaction.editReply(`Failed to use fate point: ${e?.message || String(e)}`);
        }
        return;
      }

      if (interaction.commandName === 'fpup') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const user = interaction.options.getUser('user', false);
          const discordUserId = user?.id || interaction.user?.id;
          
          if (!discordUserId) {
            await interaction.editReply('Could not resolve Discord user ID.');
            return;
          }

          const result = await darkCityApiRequest(`/api/characters/discord/by-user/${discordUserId}`);
          if (!result || !result.character) {
            await interaction.editReply('No approved character found linked to that account.');
            return;
          }

          const currentFp = result.character.fatePoints || 0;
          if (currentFp >= 5) {
            await interaction.editReply('That character already has the maximum fate points (5).');
            return;
          }

          // Update fate points by calling the character update API
          const updateResult = await darkCityApiRequest('/api/characters/discord/update-fate-points', {
            method: 'POST',
            body: JSON.stringify({ 
              discordUserId: discordUserId, 
              fatePoints: currentFp + 1 
            }),
          });

          if (updateResult && updateResult.success) {
            const characterName = result.character.name || 'Unknown';
            const targetUser = user ? `<@${user.id}>` : 'your';
            await interaction.editReply(`Added 1 fate point to **${characterName}**. ${targetUser} now have **${currentFp + 1}** fate points.`);
            logEvent('info', 'fate_point_added', 'User added 1 fate point', {
              userId: interaction.user?.id,
              targetUserId: discordUserId,
              previousFp: currentFp,
              newFp: currentFp + 1
            });
          } else {
            await interaction.editReply('Failed to update fate points. Please try again.');
          }
        } catch (e) {
          await interaction.editReply(`Failed to add fate point: ${e?.message || String(e)}`);
        }
        return;
      }

      if (interaction.commandName === 'linkcharacter') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const characterName = getNicknameCharacterName(interaction.member);
          if (!characterName) {
            await interaction.editReply('Your nickname must look like `Character Name (Player Name)` so I can detect the character name.');
            return;
          }

          const discordUserId = interaction.user?.id;
          if (!discordUserId) {
            await interaction.editReply('Could not resolve your Discord user ID.');
            return;
          }

          await darkCityApiRequest('/api/characters/discord/link', {
            method: 'POST',
            body: JSON.stringify({ discordUserId, characterName }),
          });

          await interaction.editReply(`Linked you to **${characterName}**. You can now use /card.`);
          return;
        } catch (e) {
          await interaction.editReply(`Failed to link character: ${e?.message || String(e)}`);
          return;
        }
      }

      if (interaction.commandName === 'awardxp') {
        if (!(await requireWriterOrMod(interaction))) return;
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const user = interaction.options.getUser('user', true);
          const amount = interaction.options.getInteger('amount', true);
          const result = await darkCityApiRequest('/api/characters/discord/award-xp', {
            method: 'POST',
            body: JSON.stringify({ discordUserId: user.id, amount }),
          });

          const c = result?.character;
          const leveledUp = Boolean(result?.leveledUp);
          const current = result?.current;
          const next = result?.next;
          const nextLine = next?.nextLevel
            ? `Next: level ${next.nextLevel} at ${next.nextLevelXp} XP (${next.remaining} more)`
            : 'Max level reached.';

          await interaction.editReply(
            `Awarded **${amount}** XP to <@${user.id}> (${c?.name || 'character'}).\n` +
              `Now: level **${current?.level ?? '?'}** / XP **${current?.xp ?? '?'}**` +
              (leveledUp ? '\n**Level up!** Character sheet was refreshed.' : '') +
              `\n${nextLine}`
          );
          return;
        } catch (e) {
          await interaction.editReply(`Failed to award XP: ${e?.message || String(e)}`);
          return;
        }
      }

      if (interaction.commandName === 'r') {
        const now = Date.now();
        const userId = interaction.user?.id;
        const channelId = interaction.channelId;

        const userRemaining = getCooldownRemainingMs(lastRollByUser, userId, rCooldownUserMs, now);
        const channelRemaining = getCooldownRemainingMs(lastRollByChannel, channelId, rCooldownChannelMs, now);
        const remaining = Math.max(userRemaining, channelRemaining);

        if (remaining > 0) {
          const seconds = Math.ceil(remaining / 1000);
          await interaction.reply({
            content: `⏳ Slow down! Try again in ${seconds}s.`,
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        if (userId) lastRollByUser.set(userId, now);
        if (channelId) lastRollByChannel.set(channelId, now);
        pruneOldEntries(lastRollByUser, Math.max(rCooldownUserMs, 60000) * 10, now);
        pruneOldEntries(lastRollByChannel, Math.max(rCooldownChannelMs, 60000) * 10, now);

        const { d1, d2, total } = roll2d6();
        await interaction.reply(`🎲 2d6: ${d1} + ${d2} = **${total}**`);

        logEvent('info', 'roll_2d6', 'Rolled 2d6', {
          userId,
          channelId,
          d1,
          d2,
          total,
        });
        return;
      }

      if (interaction.commandName === 'rskill') {
        const now = Date.now();
        const userId = interaction.user?.id;
        const channelId = interaction.channelId;
        const skillName = interaction.options.getString('skill', true);

        const userRemaining = getCooldownRemainingMs(lastRollByUser, userId, rCooldownUserMs, now);
        const channelRemaining = getCooldownRemainingMs(lastRollByChannel, channelId, rCooldownChannelMs, now);
        const remaining = Math.max(userRemaining, channelRemaining);

        if (remaining > 0) {
          const seconds = Math.ceil(remaining / 1000);
          await interaction.reply({
            content: `⏳ Slow down! Try again in ${seconds}s.`,
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        if (userId) lastRollByUser.set(userId, now);
        if (channelId) lastRollByChannel.set(channelId, now);
        pruneOldEntries(lastRollByUser, Math.max(rCooldownUserMs, 60000) * 10, now);
        pruneOldEntries(lastRollByChannel, Math.max(rCooldownChannelMs, 60000) * 10, now);

        try {
          // Fetch character data from API
          const characterUrl = `${DARK_CITY_API_BASE_URL}/api/characters/discord/by-user/${userId}`;
          const characterResponse = await fetch(characterUrl, {
            method: 'GET',
            headers: {
              'Content-Type': 'application/json',
            },
            timeout: 5000,
          });

          let skillBonus = 0;
          let characterName = null;

          if (characterResponse.ok) {
            const character = await characterResponse.json();
            characterName = character.name;
            
            // Find the skill bonus
            if (character.skills && Array.isArray(character.skills)) {
              const skill = character.skills.find(s => 
                s.name && s.name.toLowerCase() === skillName.toLowerCase()
              );
              if (skill) {
                skillBonus = skill.level || 0;
              }
              // If no skill found, skillBonus remains 0
            }
          }
          // If no character found, skillBonus remains 0

          const { d1, d2, total } = roll2d6();
          const finalTotal = total + skillBonus;

          let response = `🎲 2d6: ${d1} + ${d2} = **${total}**`;
          
          if (skillBonus > 0) {
            response += ` + ${skillName} (+${skillBonus}) = **${finalTotal}**`;
            if (characterName) {
              response += `\n*Roll for ${characterName}*`;
            }
          } else {
            response += ` + ${skillName} (+0) = **${finalTotal}**`;
            if (characterName) {
              response += `\n*Roll for ${characterName} (no ${skillName} skill bonus)*`;
            } else {
              response += `\n*No character linked (no skill bonus)*`;
            }
          }

          await interaction.reply(response);

          logEvent('info', 'roll_2d6_skill', 'Rolled 2d6 with character skill bonus', {
            userId,
            channelId,
            skillName,
            skillBonus,
            characterName,
            d1,
            d2,
            total,
            finalTotal,
          });
        } catch (error) {
          console.error('Error fetching character for rskill command:', error);
          
          // Fallback to normal roll with 0 bonus if API fails
          const { d1, d2, total } = roll2d6();
          await interaction.reply(`🎲 2d6: ${d1} + ${d2} = **${total}** + ${skillName} (+0) = **${total}**\n*Could not fetch character data (no skill bonus)*`);

          logEvent('error', 'roll_2d6_skill_error', 'Error in rskill command', {
            userId,
            channelId,
            skillName,
            error: error.message,
          });
        }
        return;
      }

      if (interaction.commandName === 'start') {
        const channelId = interaction.channelId;
        const playersString = interaction.options.getString('players', true);
        const dateString = interaction.options.getString('date', false);

        // Check if there's already an active scene in this channel
        if (activeScenesByChannel.has(channelId)) {
          await interaction.reply({
            content: '❌ There is already an active scene in this channel. Use `/end` to end it first.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        await interaction.deferReply();

        try {
          // Parse the date
          const sceneDate = parseDate(dateString);
          if (!sceneDate) {
            await interaction.editReply({
              content: '❌ Invalid date format. Please use DD/MM/YYYY format (e.g., 25/12/2024).',
            });
            return;
          }

          // Extract user IDs from mentions
          const userIds = extractUserIds(playersString);
          if (userIds.length === 0) {
            await interaction.editReply({
              content: '❌ No valid player mentions found. Please mention players with @ (e.g., @player1 @player2).',
            });
            return;
          }

          // Get character names for all mentioned users
          const characterNames = await getCharacterNames(userIds);

          // Create the start message
          const startMessage = await interaction.editReply({
            content: `🎬 **Scene Started**\n**Date:** ${sceneDate.toLocaleDateString('en-GB')}\n**Players:** ${characterNames.join(', ')}\n\n*Use /end when the scene is complete.*`,
          });

          // Create calendar post
          const startMessageUrl = `https://discord.com/channels/${DISCORD_GUILD_ID}/${channelId}/${startMessage.id}`;
          const { calendarMessageId, calendarMessage } = await createCalendarPost(characterNames, sceneDate, startMessageUrl);

          // Store scene data
          const sceneData = {
            channelId,
            userIds,
            characterNames,
            date: sceneDate,
            startedAt: Date.now(),
            startMessageId: startMessage.id,
            calendarMessageId,
          };

          activeScenesByChannel.set(channelId, sceneData);
          sceneMessagesByChannel.set(channelId, {
            startMessageId: startMessage.id,
            calendarMessageId,
          });

          // Check fate points for all players and give 1 FP to anyone at 0
          try {
            for (const userId of userIds) {
              try {
                const result = await darkCityApiRequest(`/api/characters/discord/by-user/${userId}`);
                if (result && result.character) {
                  const currentFp = result.character.fatePoints || 0;
                  if (currentFp === 0) {
                    // Give them 1 fate point
                    await darkCityApiRequest('/api/characters/discord/update-fate-points', {
                      method: 'POST',
                      body: JSON.stringify({ 
                        discordUserId: userId, 
                        fatePoints: 1 
                      }),
                    });
                    logEvent('info', 'fate_point_granted', 'Player granted 1 fate point at scene start', {
                      userId: userId,
                      characterName: result.character.name,
                      previousFp: 0,
                      newFp: 1
                    });
                  }
                }
              } catch (fpError) {
                // Don't fail the scene start if fate point check fails
                console.warn(`Failed to check fate points for user ${userId}:`, fpError?.message || String(fpError));
              }
            }
          } catch (fpCheckError) {
            // Don't fail the scene start if the whole fate point check fails
            console.warn('Fate point check failed during scene start:', fpCheckError?.message || String(fpCheckError));
          }

          logEvent('info', 'scene_started', 'Scene started', {
            channelId,
            userIds,
            characterNames,
            date: sceneDate.toISOString(),
            startMessageId: startMessage.id,
            calendarMessageId,
          });

        } catch (error) {
          console.error('Error in /start command:', error);
          await interaction.editReply({
            content: '❌ An error occurred while starting the scene. Please try again.',
          });
        }
        return;
      }

      if (interaction.commandName === 'end') {
        const channelId = interaction.channelId;

        // Check if there's an active scene in this channel
        if (!activeScenesByChannel.has(channelId)) {
          await interaction.reply({
            content: '❌ No active scene found in this channel.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        await interaction.deferReply();

        try {
          const sceneData = activeScenesByChannel.get(channelId);
          const messages = sceneMessagesByChannel.get(channelId);

          // Calculate scene duration
          const duration = Date.now() - sceneData.startedAt;
          const durationMinutes = Math.floor(duration / 60000);

          // Update the start message to show the scene is complete
          if (messages && messages.startMessageId) {
            try {
              const startMessage = await interaction.channel.messages.fetch(messages.startMessageId);
              if (startMessage) {
                const updatedContent = startMessage.content.replace(
                  '*Use /end when the scene is complete.*',
                  `✅ **Scene Complete** - Duration: ${durationMinutes} minutes`
                );
                await startMessage.edit(updatedContent);
              }
            } catch (error) {
              console.error('Error updating start message:', error);
            }
          }

          // Update calendar post (this would integrate with your calendar system)
          // For now, we'll just log it
          const updatedCalendarMessage = `📅 **Scene Completed**\n**Date:** ${sceneData.date.toLocaleDateString('en-GB')}\n**Players:** ${sceneData.characterNames.join(', ')}\n**Duration:** ${durationMinutes} minutes\n\n*Status: Completed*`;

          // Clean up scene data
          activeScenesByChannel.delete(channelId);
          sceneMessagesByChannel.delete(channelId);

          await interaction.editReply({
            content: `✅ Scene ended! Duration: ${durationMinutes} minutes.\n\nThe calendar post has been updated to show the scene as complete.`,
          });

          logEvent('info', 'scene_ended', 'Scene ended', {
            channelId,
            sceneData,
            duration,
            durationMinutes,
          });

        } catch (error) {
          console.error('Error in /end command:', error);
          await interaction.editReply({
            content: '❌ An error occurred while ending the scene. Please try again.',
          });
        }
        return;
      }

      if (interaction.commandName === 'purge') {
        if (!(await requireModerator(interaction))) return;
        const count = interaction.options.getInteger('count', true);
        const channel = interaction.channel;
        if (!channel || !channel.isTextBased()) {
          await interaction.reply({ content: 'This command can only be used in text channels.', flags: MessageFlags.Ephemeral });
          return;
        }

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        const messages = await channel.messages.fetch({ limit: Math.min(100, Math.max(1, count)) });
        const deleted = await channel.bulkDelete(messages, true);

        await interaction.editReply(`🧹 Deleted ${deleted.size} messages.`);
        logEvent('info', 'mod_purge', 'Purged messages', {
          userId: interaction.user?.id,
          channelId: interaction.channelId,
          count,
          deleted: deleted.size,
        });
        return;
      }

      if (interaction.commandName === 'timeout' || interaction.commandName === 'untimeout') {
        if (!(await requireModerator(interaction))) return;
        const target = interaction.options.getUser('user', true);
        const reason = interaction.options.getString('reason', false) || undefined;
        const guild = interaction.guild;
        if (!guild) {
          await interaction.reply({ content: 'This command must be used in a server.', flags: MessageFlags.Ephemeral });
          return;
        }

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        const member = await guild.members.fetch(target.id);
        if (!member) {
          await interaction.editReply('Could not find that member.');
          return;
        }

        if (interaction.commandName === 'timeout') {
          const minutes = interaction.options.getInteger('minutes', true);
          const ms = minutes * 60_000;
          await member.timeout(ms, reason);
          await interaction.editReply(`⏱️ Timed out <@${target.id}> for ${minutes} minute(s).`);
          logEvent('info', 'mod_timeout', 'Timed out member', {
            userId: interaction.user?.id,
            targetId: target.id,
            minutes,
            reason: reason || null,
            channelId: interaction.channelId,
          });
          return;
        }

        await member.timeout(null, reason);
        await interaction.editReply(`✅ Removed timeout for <@${target.id}>.`);
        logEvent('info', 'mod_untimeout', 'Removed timeout', {
          userId: interaction.user?.id,
          targetId: target.id,
          reason: reason || null,
          channelId: interaction.channelId,
        });
        return;
      }

      if (interaction.commandName === 'slowmode') {
        if (!(await requireModerator(interaction))) return;
        const seconds = interaction.options.getInteger('seconds', true);
        const channel = interaction.channel;
        if (!channel || typeof channel.setRateLimitPerUser !== 'function') {
          await interaction.reply({ content: 'This command can only be used in a text channel.', flags: MessageFlags.Ephemeral });
          return;
        }

        await channel.setRateLimitPerUser(seconds);
        await interaction.reply({ content: `🐢 Slowmode set to ${seconds}s.`, flags: MessageFlags.Ephemeral });
        logEvent('info', 'mod_slowmode', 'Set slowmode', {
          userId: interaction.user?.id,
          channelId: interaction.channelId,
          seconds,
        });
        return;
      }

      if (interaction.commandName === 'lock' || interaction.commandName === 'unlock') {
        if (!(await requireModerator(interaction))) return;
        const guild = interaction.guild;
        const channel = interaction.channel;
        const reason = interaction.options.getString('reason', false) || undefined;

        if (!guild || !channel || typeof channel.permissionOverwrites?.edit !== 'function') {
          await interaction.reply({ content: 'This command can only be used in a server channel.', flags: MessageFlags.Ephemeral });
          return;
        }

        const everyoneRoleId = guild.roles.everyone.id;
        const locking = interaction.commandName === 'lock';

        await channel.permissionOverwrites.edit(
          everyoneRoleId,
          {
            SendMessages: locking ? false : null,
          },
          { reason }
        );

        await interaction.reply({ content: locking ? '🔒 Channel locked.' : '🔓 Channel unlocked.', flags: MessageFlags.Ephemeral });
        logEvent('info', locking ? 'mod_lock' : 'mod_unlock', locking ? 'Locked channel' : 'Unlocked channel', {
          userId: interaction.user?.id,
          channelId: interaction.channelId,
          reason: reason || null,
        });
        return;
      }

      if (interaction.commandName === 'reader') {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        try {
          const member = interaction.member;
          if (!member || typeof member.roles?.cache?.has !== 'function') {
            await interaction.editReply('Could not resolve your server roles.');
            return;
          }

          if (member.roles.cache.has(READER_ROLE_ID)) {
            await interaction.editReply('You already have the reader role!');
            return;
          }

          await member.roles.add(READER_ROLE_ID, 'Reader role requested via command');
          await interaction.editReply('✅ Reader role has been assigned to you! You can now access server content.');
          
          console.log(`Reader role assigned to ${interaction.user.tag} (${interaction.user.id}) via command`);
          logEvent('info', 'reader_role_assigned', 'Reader role assigned via command', {
            userId: interaction.user.id,
            username: interaction.user.tag,
          });
          return;
        } catch (error) {
          console.error('Reader role command error:', error);
          await interaction.editReply('Failed to assign reader role. The bot may not have sufficient permissions.');
          return;
        }
      }
    
      // Unknown command
    } catch (error) {
      console.error('Interaction error:', error);
      logEvent('error', 'interaction_error', error?.message || String(error), {
        stack: error?.stack,
      });
      if (interaction.isRepliable()) {
        const alreadyReplied = interaction.replied || interaction.deferred;
        const msg = 'Something went wrong handling that command.';
        if (alreadyReplied) await interaction.followUp({ content: msg, flags: MessageFlags.Ephemeral });
        else await interaction.reply({ content: msg, flags: MessageFlags.Ephemeral });
      }
    }
  });

  client.on('interactionCreate', async (interaction) => {
    try {
      if (!interaction.isStringSelectMenu()) return;
      if (!interaction.customId?.startsWith('aspects:')) return;
      if (!interaction.guild) return;
      if (!aspectsEnabled) {
        await interaction.reply({ content: 'Aspects are currently disabled.', flags: MessageFlags.Ephemeral });
        return;
      }

      const member = interaction.member;
      if (!member || typeof member.roles?.cache?.has !== 'function') {
        await interaction.reply({ content: 'Could not resolve your server roles.', flags: MessageFlags.Ephemeral });
        return;
      }

      const categories = readAspectsFromMarkdown();
      const { roleIdToCategoryKey, allAspectRoleIds } = await getAspectRoleMaps(interaction.guild, categories);

      const selectedRoleIds = new Set(interaction.values || []);

      const currentAspectRoles = member.roles.cache
        .filter((r) => allAspectRoleIds.has(r.id))
        .map((r) => r.id);

      const thisCategoryKey = String(interaction.customId.split(':')[1] || '');

      const keep = new Set();
      const desired = [];
      for (const rid of selectedRoleIds) desired.push(rid);

      if (desired.length >= aspectsMaxSelected) {
        const finalDesired = desired.slice(0, aspectsMaxSelected);
        for (const rid of finalDesired) keep.add(rid);
      } else {
        for (const rid of desired) keep.add(rid);
        for (const rid of currentAspectRoles) {
          if (keep.size >= aspectsMaxSelected) break;
          if (keep.has(rid)) continue;
          const catKey = roleIdToCategoryKey.get(rid);
          if (catKey === thisCategoryKey) continue;
          keep.add(rid);
        }
      }

      const rolesToRemove = [];
      for (const rid of currentAspectRoles) {
        const catKey = roleIdToCategoryKey.get(rid);
        if (catKey === thisCategoryKey && !selectedRoleIds.has(rid)) {
          rolesToRemove.push(rid);
        } else if (!keep.has(rid)) {
          rolesToRemove.push(rid);
        }
      }

      const rolesToAdd = [];
      for (const rid of keep) {
        if (!member.roles.cache.has(rid)) rolesToAdd.push(rid);
      }

      if (rolesToRemove.length === 0 && rolesToAdd.length === 0) {
        await interaction.reply({ content: `Your Aspects are unchanged. (Max ${aspectsMaxSelected})`, flags: MessageFlags.Ephemeral });
        return;
      }

      if (rolesToRemove.length > 0) await member.roles.remove(rolesToRemove, 'Dark City bot: enforce max aspects');
      if (rolesToAdd.length > 0) await member.roles.add(rolesToAdd, 'Dark City bot: aspects selected');

      logEvent('info', 'aspects_updated', 'Updated member aspects', {
        userId: interaction.user?.id,
        added: rolesToAdd,
        removed: rolesToRemove,
      });

      await interaction.reply({ content: `Updated your Aspects. (Max ${aspectsMaxSelected})`, flags: MessageFlags.Ephemeral });
    } catch (error) {
      console.error('Aspects select handler error:', error);
      logEvent('error', 'aspects_select_error', error?.message || String(error), { stack: error?.stack });
      if (interaction.isRepliable()) {
        const alreadyReplied = interaction.replied || interaction.deferred;
        const msg = 'Something went wrong updating your Aspects.';
        if (alreadyReplied) await interaction.followUp({ content: msg, flags: MessageFlags.Ephemeral });
        else await interaction.reply({ content: msg, flags: MessageFlags.Ephemeral });
      }
    }
  });

  client.on('messageCreate', async (message) => {
    try {
      if (!message) return;
      if (message.author?.bot) return;
      if (!message.guild || message.guild.id !== DISCORD_GUILD_ID) return;

      const content = message.content || '';
      if (!content) return;

      const member = message.member;
      if (hasModPermission(member)) return;

      const userId = message.author?.id;
      const channelId = message.channel?.id;

      const now = Date.now();

      if (
        xpEnabled &&
        userId &&
        isXpAllowedChannel(message) &&
        Number.isFinite(xpPerMessage) &&
        xpPerMessage !== 0
      ) {
        const minChars = Math.max(0, xpMinMessageChars);
        if (minChars === 0 || String(content).trim().length >= minChars) {
          const cooldownMs = Math.max(0, xpCooldownSeconds) * 1000;
          const lastAt = lastXpAwardAtByUser.get(userId) || 0;
          if (cooldownMs === 0 || (now - lastAt) >= cooldownMs) {
            lastXpAwardAtByUser.set(userId, now);
            pruneOldEntries(lastXpAwardAtByUser, Math.max(60_000, cooldownMs) * 10, now);

            try {
              await darkCityApiRequest('/api/characters/discord/award-xp', {
                method: 'POST',
                body: JSON.stringify({ discordUserId: userId, amount: xpPerMessage }),
              });
              logEvent('info', 'xp_awarded', 'Awarded XP for message activity', {
                userId,
                channelId,
                amount: xpPerMessage,
              });
            } catch (e) {
              logEvent('warn', 'xp_award_failed', e?.message || String(e), {
                userId,
                channelId,
              });
            }
          }
        }
      }

      // Phase 1: invite links
      if (inviteAutoDeleteEnabled && INVITE_REGEX.test(content)) {
        try {
          await message.delete();
        } catch (e) {
          console.error('Failed to delete invite message:', e);
          logEvent('error', 'automod_invite_delete_failed', e?.message || String(e), {
            userId,
            channelId,
            messageId: message.id,
          });
          return;
        }

        logEvent('info', 'automod_invite_deleted', 'Deleted Discord invite link', {
          userId,
          channelId,
          messageId: message.id,
        });

        if (inviteWarnEnabled && userId && message.channel && message.channel.isTextBased()) {
          const lastWarn = lastInviteWarnByUser.get(userId) || 0;
          if (now - lastWarn >= 60_000) {
            lastInviteWarnByUser.set(userId, now);
            pruneWarnEntries(now);

            const warn = await message.channel.send(
              `⚠️ <@${userId}> invite links aren’t allowed here. If you think this was a mistake, message a moderator.`
            );

            const delayMs = Math.max(0, Math.min(120, inviteWarnDeleteSeconds)) * 1000;
            if (delayMs > 0) {
              setTimeout(() => {
                warn.delete().catch(() => {});
              }, delayMs);
            }
          }
        }

        return;
      }

      // Phase 2: low-trust link filter (account age)
      if (lowTrustLinkFilterEnabled && URL_REGEX.test(content) && isLowTrustAccount(message.author, now)) {
        try {
          await message.delete();
        } catch (e) {
          console.error('Failed to delete low-trust link message:', e);
          logEvent('error', 'automod_lowtrust_link_delete_failed', e?.message || String(e), {
            userId,
            channelId,
            messageId: message.id,
          });
          return;
        }

        logEvent('info', 'automod_lowtrust_link_deleted', 'Deleted link from low-trust account', {
          userId,
          channelId,
          messageId: message.id,
          minAccountAgeDays: lowTrustMinAccountAgeDays,
        });

        if (lowTrustWarnDmEnabled && userId) {
          const lastWarn = lastLowTrustDmWarnByUser.get(userId) || 0;
          if (now - lastWarn >= 60_000) {
            lastLowTrustDmWarnByUser.set(userId, now);
            pruneWarnEntries(now);

            try {
              await message.author.send(
                `Your message in **${message.guild.name}** was removed because new accounts can’t post links yet. ` +
                  `Please wait until your account is at least **${lowTrustMinAccountAgeDays} day(s)** old, ` +
                  `or message a moderator if you think this was a mistake.`
              );
            } catch (e) {
              logEvent('warn', 'automod_lowtrust_dm_failed', e?.message || String(e), { userId });
            }
          }
        }

        return;
      }

      if (
        spamAutoModEnabled &&
        userId &&
        channelId &&
        !spamIgnoredChannelIds.includes(channelId)
      ) {
        if (spamBypassRoleIds.length > 0 && message.member?.roles?.cache) {
          const hasBypass = spamBypassRoleIds.some((rid) => message.member.roles.cache.has(rid));
          if (hasBypass) return;
        }

        const floodWindowMs = Math.max(1, spamFloodWindowSeconds) * 1000;
        const repeatWindowMs = Math.max(1, spamRepeatWindowSeconds) * 1000;

        const tsList = recentMessageTimestampsByUser.get(userId) || [];
        const kept = tsList.filter((ts) => now - ts <= Math.max(floodWindowMs, repeatWindowMs));
        kept.push(now);
        recentMessageTimestampsByUser.set(userId, kept);

        const withinFlood = kept.filter((ts) => now - ts <= floodWindowMs);
        const floodTriggered = withinFlood.length > Math.max(1, spamFloodMaxMessages);

        const norm = normalizeForRepeatCheck(content);
        const last = lastMessageNormByUser.get(userId) || { norm: '', lastAt: 0, repeats: 0 };
        let repeats = 0;
        if (norm && last.norm && norm === last.norm && (now - last.lastAt) <= repeatWindowMs) {
          repeats = (last.repeats || 0) + 1;
        }
        lastMessageNormByUser.set(userId, { norm, lastAt: now, repeats });

        const repeatTriggered = repeats >= Math.max(1, spamRepeatMaxRepeats);

        if (floodTriggered || repeatTriggered) {
          try {
            await message.delete();
          } catch (e) {
            logEvent('error', 'automod_spam_delete_failed', e?.message || String(e), {
              userId,
              channelId,
              messageId: message.id,
              floodTriggered,
              repeatTriggered,
            });
          }

          const reason = floodTriggered ? 'message flood' : 'repeated messages';
          logEvent('info', 'automod_spam_deleted', 'Deleted spam message', {
            userId,
            channelId,
            messageId: message.id,
            reason,
            floodCountInWindow: withinFlood.length,
            repeats,
          });

          const decayMs = Math.max(0, spamStrikeDecayMinutes) * 60_000;
          const prev = spamStrikeCountByUser.get(userId) || { count: 0, lastAt: 0 };
          const prevCount = (decayMs > 0 && prev.lastAt && (now - prev.lastAt) > decayMs) ? 0 : (prev.count || 0);
          const strikes = prevCount + 1;
          spamStrikeCountByUser.set(userId, { count: strikes, lastAt: now });

          if (spamWarnEnabled && message.channel && message.channel.isTextBased()) {
            const lastWarn = lastSpamWarnByUser.get(userId) || 0;
            if (now - lastWarn >= 20_000) {
              lastSpamWarnByUser.set(userId, now);
              pruneWarnEntries(now);
              const warn = await message.channel.send(
                `⚠️ <@${userId}> please slow down — spam (${reason}) isn’t allowed. Continued spam may result in a timeout.`
              );
              const delayMs = Math.max(0, Math.min(120, spamWarnDeleteSeconds)) * 1000;
              if (delayMs > 0) {
                setTimeout(() => {
                  warn.delete().catch(() => {});
                }, delayMs);
              }
            }
          }

          if (spamTimeoutEnabled && spamTimeoutMinutes > 0 && strikes >= 2) {
            const ms = Math.min(28 * 24 * 60, Math.max(1, spamTimeoutMinutes)) * 60_000;
            try {
              if (message.member?.timeout) {
                await message.member.timeout(ms, `Auto-mod: spam (${reason})`);
                logEvent('info', 'automod_spam_timeout', 'Timed out member for spam', {
                  userId,
                  channelId,
                  minutes: spamTimeoutMinutes,
                  strikes,
                  reason,
                });
              }
            } catch (e) {
              logEvent('error', 'automod_spam_timeout_failed', e?.message || String(e), {
                userId,
                channelId,
                minutes: spamTimeoutMinutes,
                strikes,
                reason,
              });
            }
          }

          return;
        }
      }

    } catch (error) {
      console.error('messageCreate handler error:', error);
      logEvent('error', 'message_create_error', error?.message || String(error), { stack: error?.stack });
    }
  });

  // Message-based reader role command as backup
  client.on('messageCreate', async (message) => {
    try {
      if (message.author?.bot) return;
      if (!message.guild || message.guild.id !== DISCORD_GUILD_ID) return;
      
      const content = message.content?.trim().toLowerCase();
      if (content === '!reader') {
        const member = message.member;
        if (!member || typeof member.roles?.cache?.has !== 'function') {
          await message.reply('Could not resolve your server roles.');
          return;
        }

        if (member.roles.cache.has(READER_ROLE_ID)) {
          await message.reply('You already have the reader role!');
          return;
        }

        try {
          await member.roles.add(READER_ROLE_ID, 'Reader role requested via message command');
          await message.reply('✅ Reader role has been assigned to you! You can now access server content.');
          
          console.log(`Reader role assigned to ${message.author.tag} (${message.author.id}) via message command`);
          logEvent('info', 'reader_role_assigned_message', 'Reader role assigned via message command', {
            userId: message.author.id,
            username: message.author.tag,
          });
        } catch (roleError) {
          console.error('Reader role message command error:', roleError);
          await message.reply('Failed to assign reader role. The bot may not have sufficient permissions.');
        }
        return;
      }
    } catch (error) {
      console.error('Message reader command error:', error);
    }
  });

  await client.login(DISCORD_BOT_TOKEN);
}

main().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
