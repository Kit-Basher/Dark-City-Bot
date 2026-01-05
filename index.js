const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } = require('discord.js');

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

const rollCommand = new SlashCommandBuilder()
  .setName('r')
  .setDescription('Roll 2d6');

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(DISCORD_BOT_TOKEN);
  await rest.put(Routes.applicationGuildCommands(DISCORD_APPLICATION_ID, DISCORD_GUILD_ID), {
    body: [rollCommand.toJSON()],
  });
}

function roll2d6() {
  const d1 = Math.floor(Math.random() * 6) + 1;
  const d2 = Math.floor(Math.random() * 6) + 1;
  return { d1, d2, total: d1 + d2 };
}

async function main() {
  await registerCommands();

  const client = new Client({
    intents: [GatewayIntentBits.Guilds],
  });

  client.once('ready', () => {
    console.log(`🤖 Logged in as ${client.user.tag}`);
  });

  client.on('interactionCreate', async (interaction) => {
    try {
      if (!interaction.isChatInputCommand()) return;
      if (interaction.commandName !== 'r') return;

      const { d1, d2, total } = roll2d6();
      await interaction.reply(`🎲 2d6: ${d1} + ${d2} = **${total}**`);
    } catch (error) {
      console.error('Interaction error:', error);
      if (interaction.isRepliable()) {
        const alreadyReplied = interaction.replied || interaction.deferred;
        const msg = 'Something went wrong handling that command.';
        if (alreadyReplied) await interaction.followUp({ content: msg, ephemeral: true });
        else await interaction.reply({ content: msg, ephemeral: true });
      }
    }
  });

  await client.login(DISCORD_BOT_TOKEN);
}

main().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
