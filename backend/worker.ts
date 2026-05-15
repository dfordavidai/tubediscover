/**
 * TubeDiscover — worker.ts
 * ─────────────────────────────────────────────────────────
 * ONE FILE. Every BullMQ queue, every worker, every cron.
 * Run: npx tsx worker.ts  (dev)
 *      node dist/worker.js (prod after tsc)
 *
 * This process is completely independent of server.ts.
 * If the API crashes, crawling keeps running. If workers crash,
 * the API keeps serving. That's the whole point.
 *
 * All queues are exported so server.ts can enqueue jobs
 * (e.g. POST /api/videos/index) without a circular dependency.
 */

import 'dotenv/config';
import { Queue, Worker, Job } from 'bullmq';
import cron from 'node-cron';
import axios from 'axios';
import { parseStringPromise } from 'xml2js';
import pLimit from 'p-limit';

// Re-use shared singletons from server — import only what we need
// (These are safe to import because they're plain exports, no Fastify side-effects)
import {
  prisma,
  redis,
  logger,
  upsertVideo,
  batchHotScoreUpdateSql,
  type VideoMeta,
} from './server.js';

// ─────────────────────────────────────────────────────────
// HTTP CLIENT
// ─────────────────────────────────────────────────────────

const http = axios.create({
  timeout: 15000,
  headers: { 'User-Agent': 'Mozilla/5.0 (compatible; TubeDiscover/1.0; +https://tubediscover.com/bot)' },
});

// ─────────────────────────────────────────────────────────
// QUEUE DEFINITIONS  (exported so server.ts can enqueue)
// ─────────────────────────────────────────────────────────

const connection = redis;

export const crawlQueue = new Queue('crawl', {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 5000 },
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 50 },
  },
});

export const indexQueue = new Queue('index', {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 3000 },
    removeOnComplete: { count: 200 },
    removeOnFail: { count: 100 },
  },
});

export const rssQueue = new Queue('rss', {
  connection,
  defaultJobOptions: {
    attempts: 2,
    backoff: { type: 'fixed', delay: 10000 },
    removeOnComplete: { count: 50 },
    removeOnFail: { count: 20 },
  },
});

export type CrawlJobData = { keyword: string; category?: string; priority?: number };
export type IndexJobData = { youtubeId: string; source: 'search' | 'rss' | 'manual' };
export type RssJobData   = { channelId: string };

// ─────────────────────────────────────────────────────────
// YOUTUBE HELPERS  (no API key — scraping + RSS + oEmbed)
// ─────────────────────────────────────────────────────────

export interface RssFeedItem {
  youtubeId: string; title: string; channelId: string; channelName: string;
  publishedAt: Date | null; thumbnail: string; description: string;
}

/** Parse a YouTube channel RSS feed */
export async function fetchChannelRss(channelId: string): Promise<RssFeedItem[]> {
  try {
    const url = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
    const { data } = await http.get(url);
    const parsed = await parseStringPromise(data, { explicitArray: false });
    const feed = parsed?.feed;
    if (!feed?.entry) return [];

    const entries = Array.isArray(feed.entry) ? feed.entry : [feed.entry];
    const channelName = feed?.author?.name || '';
    const feedChannelId = feed?.['yt:channelId'] || channelId;

    return entries.map((e: Record<string, unknown>) => {
      const mediaGroup = e['media:group'] as Record<string, unknown> | undefined;
      const thumbnail  = mediaGroup?.['media:thumbnail'] as { $?: { url?: string } } | undefined;
      const description = mediaGroup?.['media:description'] as string | undefined;
      return {
        youtubeId:   (e['yt:videoId']    as string) || '',
        title:       (e.title            as string) || '',
        channelId:   (e['yt:channelId']  as string) || feedChannelId,
        channelName,
        publishedAt: e.published ? new Date(e.published as string) : null,
        thumbnail:   thumbnail?.$?.url || `https://i.ytimg.com/vi/${e['yt:videoId']}/hqdefault.jpg`,
        description: description || '',
      };
    }).filter((v: RssFeedItem) => v.youtubeId);
  } catch (err) {
    logger.error('RSS fetch failed', { channelId, err: (err as Error).message });
    return [];
  }
}

/** Fetch video IDs by scraping YouTube search results HTML */
export async function searchYouTube(keyword: string, maxResults = 20): Promise<string[]> {
  try {
    const url = `https://www.youtube.com/results?search_query=${encodeURIComponent(keyword)}&sp=CAASAhAB`;
    const { data } = await http.get(url);
    const regex = /"videoId":"([a-zA-Z0-9_-]{11})"/g;
    const ids = new Set<string>();
    let match;
    while ((match = regex.exec(data)) !== null) {
      ids.add(match[1]);
      if (ids.size >= maxResults) break;
    }
    return [...ids];
  } catch (err) {
    logger.error('YouTube search scrape failed', { keyword, err: (err as Error).message });
    return [];
  }
}

/** YouTube autocomplete suggestions */
export async function fetchSearchSuggestions(query: string): Promise<string[]> {
  try {
    const url = `https://suggestqueries.google.com/complete/search?client=youtube&ds=yt&q=${encodeURIComponent(query)}`;
    const { data } = await http.get(url);
    const jsonStart = data.indexOf('[');
    if (jsonStart === -1) return [];
    const parsed = JSON.parse(data.slice(jsonStart));
    return ((parsed[1] || []) as unknown[][])
      .map((item) => item[0] as string)
      .filter(Boolean)
      .slice(0, 10);
  } catch {
    return [];
  }
}

/** Scrape video metadata from the watch page */
export async function scrapeVideoPage(youtubeId: string): Promise<Partial<VideoMeta>> {
  try {
    const { data } = await http.get(`https://www.youtube.com/watch?v=${youtubeId}`);

    const extract = (pattern: RegExp) => data.match(pattern)?.[1] || '';

    const title = extract(/"title":"([^"]+)"/)
      .replace(/\\u[\dA-F]{4}/gi, (m: string) => String.fromCharCode(parseInt(m.replace(/\\u/i, ''), 16)));
    const channelName  = extract(/"ownerChannelName":"([^"]+)"/);
    const channelId    = extract(/"channelId":"([^"]+)"/);
    const views        = parseInt(extract(/"viewCount":"(\d+)"/) || '0');
    const likeRaw      = extract(/"label":"([\d,]+) likes"/).replace(/,/g, '');
    const likes        = likeRaw ? parseInt(likeRaw) : 0;
    const description  = extract(/"shortDescription":"([^"]{0,500})"/);
    const category     = extract(/"category":"([^"]+)"/) || 'Entertainment';
    const duration     = parseInt(extract(/"lengthSeconds":"(\d+)"/) || '0') || null;
    const dateRaw      = extract(/"publishDate":"([^"]+)"/);
    const publishedAt  = dateRaw ? new Date(dateRaw) : null;

    const tagsMatch = data.match(/"keywords":\[([^\]]+)\]/);
    const tags = tagsMatch
      ? tagsMatch[1].split(',').map((t: string) => t.replace(/"/g, '').trim()).filter(Boolean).slice(0, 20)
      : [];

    return {
      youtubeId, title, channelName, channelId, views, likes,
      description, tags, category, duration, publishedAt,
      thumbnail: `https://i.ytimg.com/vi/${youtubeId}/hqdefault.jpg`,
    };
  } catch (err) {
    logger.error('Video page scrape failed', { youtubeId, err: (err as Error).message });
    return {};
  }
}

/** Fallback: oEmbed gives title + channel name, no key needed */
export async function fetchOEmbed(youtubeId: string): Promise<Partial<VideoMeta>> {
  try {
    const url = `https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${youtubeId}&format=json`;
    const { data } = await http.get(url);
    return { youtubeId, title: data.title || '', channelName: data.author_name || '', thumbnail: `https://i.ytimg.com/vi/${youtubeId}/hqdefault.jpg`, tags: [], views: 0, likes: 0 };
  } catch {
    return { youtubeId, thumbnail: `https://i.ytimg.com/vi/${youtubeId}/hqdefault.jpg` };
  }
}

// ─────────────────────────────────────────────────────────
// SEED DATA
// ─────────────────────────────────────────────────────────

export const SEED_CHANNELS = [
  'UCVHFbw7woebKtYre-sb9_Dg', // ESPN
  'UCbmNph6atAoGfqLoCL_duAg', // Pitchfork
  'UCzWQYUVCpZqtN93H8RR44Qw', // BBC News
  'UCHnyfMqiRRG1u-2MsSQLbXA', // Veritasium
  'UC9-y-6csu5WGm29I7JiwpnA', // Computerphile
  'UCJXGnJCfRFMuFa6bMQnGBhQ', // Kurzgesagt
  'UCBcRF18a7Qf58cCRy5xuWwQ', // MrBeast
  'UCq-Fj5jknLsUf-MWSy4_brA', // TED
  'UCiGm_E4Ze_76W5Q7BoZQFAA', // NBC News
  'UCupvZG-5ko_eiXAupbDfxWw', // CNN
];

// ─────────────────────────────────────────────────────────
// CRAWL WORKER  — keyword → YouTube search → index jobs
// ─────────────────────────────────────────────────────────

const searchConcurrency = pLimit(3);

function startCrawlWorker() {
  const worker = new Worker<CrawlJobData>(
    'crawl',
    async (job: Job<CrawlJobData>) => {
      const { keyword, category } = job.data;
      logger.info('Crawling keyword', { keyword });

      const videoIds = await searchYouTube(keyword, 20);
      logger.info('Found videos', { keyword, count: videoIds.length });

      if (videoIds.length > 0) {
        await indexQueue.addBulk(
          videoIds.map((youtubeId) => ({ name: `index:${youtubeId}`, data: { youtubeId, source: 'search' } as IndexJobData }))
        );
      }

      // Autocomplete expansion — discover new keywords
      const suggestions = await fetchSearchSuggestions(keyword);
      const newKeywords = suggestions.filter((s) => s.length > 2 && s.length < 80);

      if (newKeywords.length > 0) {
        await Promise.all(
          newKeywords.map((kw) =>
            searchConcurrency(() =>
              prisma.keyword.upsert({
                where: { keyword: kw },
                create: { keyword: kw, category: category || 'general', priority: 2 },
                update: {},
              })
            )
          )
        );
        logger.info('Discovered new keywords', { count: newKeywords.length });
      }

      await prisma.keyword.updateMany({
        where: { keyword },
        data: { crawlCount: { increment: 1 }, lastCrawled: new Date() },
      });
    },
    { connection: redis, concurrency: 2 }
  );

  worker.on('completed', (job) => logger.debug('Crawl done', { keyword: job.data.keyword }));
  worker.on('failed', (job, err) => logger.error('Crawl failed', { keyword: job?.data.keyword, err: err.message }));
  return worker;
}

// ─────────────────────────────────────────────────────────
// INDEX WORKER  — youtubeId → scrape → upsert DB + Meili
// ─────────────────────────────────────────────────────────

function startIndexWorker() {
  const worker = new Worker<IndexJobData>(
    'index',
    async (job: Job<IndexJobData>) => {
      const { youtubeId } = job.data;
      let meta = await scrapeVideoPage(youtubeId);
      if (!meta.title) {
        logger.debug('Falling back to oEmbed', { youtubeId });
        const oembed = await fetchOEmbed(youtubeId);
        meta = { ...oembed, ...meta };
      }
      if (!meta.youtubeId) meta.youtubeId = youtubeId;
      const success = await upsertVideo(meta);
      if (success) logger.debug('Video indexed', { youtubeId, title: meta.title });
    },
    {
      connection: redis,
      concurrency: 5,
      limiter: { max: 10, duration: 1000 }, // max 10/sec to avoid YouTube rate-limit
    }
  );

  worker.on('failed', (job, err) => logger.error('Index failed', { youtubeId: job?.data.youtubeId, err: err.message }));
  return worker;
}

// ─────────────────────────────────────────────────────────
// RSS WORKER  — channelId → parse feed → upsert videos
// ─────────────────────────────────────────────────────────

function startRssWorker() {
  const worker = new Worker<RssJobData>(
    'rss',
    async (job: Job<RssJobData>) => {
      const { channelId } = job.data;
      logger.info('Fetching RSS', { channelId });
      const items = await fetchChannelRss(channelId);
      logger.info('RSS items', { channelId, count: items.length });
      for (const item of items) {
        await upsertVideo({
          youtubeId: item.youtubeId, title: item.title, description: item.description,
          thumbnail: item.thumbnail, channelId: item.channelId, channelName: item.channelName,
          publishedAt: item.publishedAt, tags: [], views: 0, likes: 0,
        });
      }
    },
    { connection: redis, concurrency: 3 }
  );

  worker.on('failed', (job, err) => logger.error('RSS failed', { channelId: job?.data.channelId, err: err.message }));
  return worker;
}

// ─────────────────────────────────────────────────────────
// SCHEDULING HELPERS
// ─────────────────────────────────────────────────────────

/** Pull the next batch of due keywords from DB and enqueue crawl jobs */
async function scheduleCrawlJobs(batchSize = 20) {
  const keywords = await prisma.keyword.findMany({
    where: { OR: [{ lastCrawled: null }, { lastCrawled: { lt: new Date(Date.now() - 6 * 3_600_000) } }] },
    orderBy: [{ priority: 'desc' }, { crawlCount: 'asc' }],
    take: batchSize,
  });

  if (keywords.length === 0) { logger.info('No keywords to crawl'); return 0; }

  await crawlQueue.addBulk(
    keywords.map((kw) => ({ name: `crawl:${kw.keyword}`, data: { keyword: kw.keyword, category: kw.category || 'general' } as CrawlJobData }))
  );
  logger.info('Scheduled crawl jobs', { count: keywords.length });
  return keywords.length;
}

/** Enqueue RSS jobs for seed + all known DB channels */
async function scheduleRssJobs() {
  const dbChannels = await prisma.channel.findMany({ take: 100 });
  const allChannelIds = [...new Set([...SEED_CHANNELS, ...dbChannels.map((c) => c.youtubeChannelId)])];
  await rssQueue.addBulk(allChannelIds.map((channelId) => ({ name: `rss:${channelId}`, data: { channelId } })));
  logger.info('Scheduled RSS jobs', { count: allChannelIds.length });
}

// ─────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────

async function main() {
  logger.info('Starting workers…');

  const crawlWorker = startCrawlWorker();
  const indexWorker = startIndexWorker();
  const rssWorker   = startRssWorker();

  // Cron: crawl keywords every 30 minutes
  cron.schedule('*/30 * * * *', async () => {
    logger.info('Cron: scheduling keyword crawls');
    await scheduleCrawlJobs(30);
  });

  // Cron: RSS every hour
  cron.schedule('0 * * * *', async () => {
    logger.info('Cron: running RSS crawls');
    await scheduleRssJobs();
  });

  // Cron: hot score bulk update every 6 hours
  cron.schedule('0 */6 * * *', async () => {
    logger.info('Cron: updating hot scores');
    try {
      await prisma.$executeRawUnsafe(batchHotScoreUpdateSql());
      logger.info('Hot scores updated');
    } catch (err) {
      logger.error('Hot score update failed', { err: (err as Error).message });
    }
  });

  // Kick off on startup
  await scheduleCrawlJobs(10);
  await scheduleRssJobs();

  logger.info('Workers running. Crons scheduled.');

  // Graceful shutdown
  process.on('SIGTERM', async () => {
    logger.info('SIGTERM — shutting down workers…');
    await Promise.all([crawlWorker.close(), indexWorker.close(), rssWorker.close()]);
    await prisma.$disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  logger.error('Worker startup failed', { err });
  process.exit(1);
});
