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

const BUILD_STAMP = 'aspects_missing_command';

const MODERATOR_ROLE_ID = process.env.MODERATOR_ROLE_ID || process.env.DASHBOARD_ALLOWED_ROLE_ID || '';

const ASPECTS_CHANNEL_ID = process.env.ASPECTS_CHANNEL_ID || '1457635644338868317';

const DEFAULT_R_COOLDOWN_USER_MS = parseInt(process.env.R_COOLDOWN_USER_MS || '3000', 10);
const DEFAULT_R_COOLDOWN_CHANNEL_MS = parseInt(process.env.R_COOLDOWN_CHANNEL_MS || '1000', 10);

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

let aspectsEnabled = true;
let aspectsMaxSelected = 2;

const MONGODB_URI = process.env.MONGODB_URI;
let mongoClient;
let botDb;

async function initMongo() {
  if (!MONGODB_URI) {
    console.log('ℹ️ Mongo: MONGODB_URI not set; bot settings/logs disabled');
    return;
  }

  mongoClient = new MongoClient(MONGODB_URI);
  await mongoClient.connect();
  botDb = mongoClient.db(process.env.BOT_DB_NAME || 'dark_city_bot');
  console.log('✅ Mongo: Connected');
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

  if (typeof doc.aspectsEnabled === 'boolean') aspectsEnabled = doc.aspectsEnabled;
  if (Number.isFinite(doc.aspectsMaxSelected)) aspectsMaxSelected = doc.aspectsMaxSelected;
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
        aspectsEnabled: true,
        aspectsMaxSelected: 2,
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

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(DISCORD_BOT_TOKEN);
  await rest.put(Routes.applicationGuildCommands(DISCORD_APPLICATION_ID, DISCORD_GUILD_ID), {
    body: [
      rollCommand.toJSON(),
      purgeCommand.toJSON(),
      timeoutCommand.toJSON(),
      untimeoutCommand.toJSON(),
      slowmodeCommand.toJSON(),
      lockCommand.toJSON(),
      unlockCommand.toJSON(),
      aspectsPostCommand.toJSON(),
      aspectsMissingCommand.toJSON(),
      aspectsCleanupPrefixedCommand.toJSON(),
    ],
  });
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

async function requireModerator(interaction) {
  const member = interaction.member;
  const allowed = hasModPermission(member);
  if (allowed) return true;
  await interaction.reply({ content: 'Access denied (mods only).', flags: MessageFlags.Ephemeral });
  return false;
}

function roll2d6() {
  const d1 = Math.floor(Math.random() * 6) + 1;
  const d2 = Math.floor(Math.random() * 6) + 1;
  return { d1, d2, total: d1 + d2 };
}

const lastRollByUser = new Map();
const lastRollByChannel = new Map();

const lastInviteWarnByUser = new Map();
const lastLowTrustDmWarnByUser = new Map();
const lastSpamWarnByUser = new Map();

const recentMessageTimestampsByUser = new Map();
const lastMessageNormByUser = new Map();
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
  await initMongo();
  await ensureSettingsDoc();
  await loadSettings();

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
    ],
  });

  client.once('ready', () => {
    console.log(`🤖 Logged in as ${client.user.tag}`);
    console.log('🔧 Build:', {
      BUILD_STAMP,
      renderCommit: process.env.RENDER_GIT_COMMIT,
      aspectsChannelId: ASPECTS_CHANNEL_ID,
      aspectsRoleCreatesPerRun: process.env.ASPECTS_ROLE_CREATES_PER_RUN || '25',
      aspectsRoleCreateDelayMs: process.env.ASPECTS_ROLE_CREATE_DELAY_MS || '350',
    });
    logEvent('info', 'bot_ready', 'Bot logged in', {
      userTag: client.user.tag,
      rCooldownUserMs,
      rCooldownChannelMs,
    });
  });

  client.on('interactionCreate', async (interaction) => {
    try {
      if (!interaction.isChatInputCommand()) return;

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

        if (!(await requireModerator(interaction))) {
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

        if (!(await requireModerator(interaction))) {
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

      if (spamAutoModEnabled && userId && channelId) {
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

          const strikes = (spamStrikeCountByUser.get(userId) || 0) + 1;
          spamStrikeCountByUser.set(userId, strikes);

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

  await client.login(DISCORD_BOT_TOKEN);
}

main().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
