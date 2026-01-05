const {
  Client,
  GatewayIntentBits,
  PermissionsBitField,
  REST,
  Routes,
  SlashCommandBuilder,
} = require('discord.js');
const { MongoClient } = require('mongodb');

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

const DEFAULT_R_COOLDOWN_USER_MS = parseInt(process.env.R_COOLDOWN_USER_MS || '3000', 10);
const DEFAULT_R_COOLDOWN_CHANNEL_MS = parseInt(process.env.R_COOLDOWN_CHANNEL_MS || '1000', 10);

let rCooldownUserMs = DEFAULT_R_COOLDOWN_USER_MS;
let rCooldownChannelMs = DEFAULT_R_COOLDOWN_CHANNEL_MS;

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
    ],
  });
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
  await interaction.reply({ content: 'Access denied (mods only).', ephemeral: true });
  return false;
}

function roll2d6() {
  const d1 = Math.floor(Math.random() * 6) + 1;
  const d2 = Math.floor(Math.random() * 6) + 1;
  return { d1, d2, total: d1 + d2 };
}

const lastRollByUser = new Map();
const lastRollByChannel = new Map();

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

  await registerCommands();

  const client = new Client({
    intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers, GatewayIntentBits.GuildMessages],
  });

  client.once('ready', () => {
    console.log(`🤖 Logged in as ${client.user.tag}`);
    logEvent('info', 'bot_ready', 'Bot logged in', {
      userTag: client.user.tag,
      rCooldownUserMs,
      rCooldownChannelMs,
    });
  });

  client.on('interactionCreate', async (interaction) => {
    try {
      if (!interaction.isChatInputCommand()) return;

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
            ephemeral: true,
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
          await interaction.reply({ content: 'This command can only be used in text channels.', ephemeral: true });
          return;
        }

        await interaction.deferReply({ ephemeral: true });
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
          await interaction.reply({ content: 'This command must be used in a server.', ephemeral: true });
          return;
        }

        await interaction.deferReply({ ephemeral: true });
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
          await interaction.reply({ content: 'This command can only be used in a text channel.', ephemeral: true });
          return;
        }

        await channel.setRateLimitPerUser(seconds);
        await interaction.reply({ content: `🐢 Slowmode set to ${seconds}s.`, ephemeral: true });
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
          await interaction.reply({ content: 'This command can only be used in a server channel.', ephemeral: true });
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

        await interaction.reply({ content: locking ? '🔒 Channel locked.' : '🔓 Channel unlocked.', ephemeral: true });
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
        if (alreadyReplied) await interaction.followUp({ content: msg, ephemeral: true });
        else await interaction.reply({ content: msg, ephemeral: true });
      }
    }
  });

  setInterval(() => {
    loadSettings().catch((e) => console.error('Failed to reload settings:', e));
  }, 30000);

  await client.login(DISCORD_BOT_TOKEN);
}

main().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
