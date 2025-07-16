// Apify SDK - toolkit for building Apify Actors (Read more at https://docs.apify.com/sdk/js/).
import { createLinkedinScraper } from '@harvestapi/scraper';
import { Actor } from 'apify';
import { config } from 'dotenv';

config();

// this is ESM project, and as such, it requires you to specify extensions in your relative imports
// read more about this here: https://nodejs.org/docs/latest-v18.x/api/esm.html#mandatory-file-extensions
// note that we need to use `.js` even when inside TS files
// import { router } from './routes.js';

// The init() call configures the Actor for its environment. It's recommended to start every Actor with an init().
await Actor.init();

interface Input {
  profiles: string[];
  maxItems?: number;
}
// Structure of input is defined in input_schema.json
const input = await Actor.getInput<Input>();
if (!input) throw new Error('Input is missing!');
input.profiles = (input.profiles || []).filter((q) => q && !!q.trim());
if (!input.profiles?.length) {
  console.error('No search queries provided!');
  await Actor.exit();
  process.exit(0);
}

const { actorId, actorRunId, actorBuildId, userId, actorMaxPaidDatasetItems, memoryMbytes } =
  Actor.getEnv();

const client = Actor.newClient();
const user = userId ? await client.user(userId).get() : null;

const scraper = createLinkedinScraper({
  apiKey: process.env.HARVESTAPI_TOKEN!,
  baseUrl: process.env.HARVESTAPI_URL || 'https://api.harvest-api.com',
  addHeaders: {
    'x-apify-userid': userId!,
    'x-apify-actor-id': actorId!,
    'x-apify-actor-run-id': actorRunId!,
    'x-apify-actor-build-id': actorBuildId!,
    'x-apify-memory-mbytes': String(memoryMbytes),
    'x-apify-actor-max-paid-dataset-items': String(actorMaxPaidDatasetItems) || '0',
    'x-apify-username': user?.username || '',
    'x-apify-user-is-paying': (user as Record<string, any> | null)?.isPaying,
  },
});

let maxItems = Number(input.maxItems) || actorMaxPaidDatasetItems || undefined;
if (actorMaxPaidDatasetItems && maxItems && maxItems > actorMaxPaidDatasetItems) {
  maxItems = actorMaxPaidDatasetItems;
}
let totalItemsCounter = 0;
let datasetLastPushPromise: Promise<any> | undefined;

for (const profile of input.profiles) {
  const query: {
    profile: string;
  } = {
    profile,
  };

  await scraper.scrapeProfileReactions({
    query: {
      ...query,
    },
    outputType: 'callback',
    onItemScraped: async ({ item }) => {
      totalItemsCounter++;

      if (actorMaxPaidDatasetItems && totalItemsCounter > actorMaxPaidDatasetItems) {
        console.warn('Max items reached, exiting...');
        await Promise.all([datasetLastPushPromise, Actor.exit()]);
        process.exit(0);
      }

      console.info(`Scraped reaction ${item?.id}`);
      datasetLastPushPromise = Actor.pushData(item);
    },
    overrideConcurrency: 6,
    overridePageConcurrency: 1,
    maxItems,
    disableLog: true,
  });
}

await datasetLastPushPromise;

// Gracefully exit the Actor process. It's recommended to quit all Actors with an exit().
await Actor.exit();
