/**
 * TubeDiscover — server.ts
 * ─────────────────────────────────────────────────────────
 * ONE FILE. Every API route, plugin, helper, and client.
 * Run: npx tsx server.ts  (dev)
 *      node dist/server.js (prod after tsc)
 *
 * Required .env vars:
 *   DATABASE_URL          Postgres connection string
 *   REDIS_URL             Redis URL           (default: redis://localhost:6379)
 *   MEILI_HOST            Meilisearch URL     (default: http://localhost:7700)
 *   MEILI_MASTER_KEY      Meilisearch key
 *   GROQ_API_KEY_1 …      Up to 10 Groq keys  (also accepts GROQ_API_KEY)
 *   PORT                  HTTP port           (default: 4000)
 *   HOST                  Bind host           (default: 0.0.0.0)
 *   NEXT_PUBLIC_SITE_URL  Frontend URL for CORS
 *   NODE_ENV              production | development
 */

import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import { MeiliSearch } from 'meilisearch';
import Groq from 'groq-sdk';
import winston from 'winston';
import slugify from 'slugify';
import type { FastifyInstance } from 'fastify';

// ─────────────────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────────────────

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.colorize(),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : '';
      return `${timestamp} [${level}]: ${message}${metaStr}`;
    })
  ),
  transports: [new winston.transports.Console()],
});

// ─────────────────────────────────────────────────────────
// CLIENTS
// ─────────────────────────────────────────────────────────

// Prisma
const globalForPrisma = globalThis as unknown as { prisma: PrismaClient };
export const prisma =
  globalForPrisma.prisma ||
  new PrismaClient({ log: [{ emit: 'event', level: 'error' }, { emit: 'event', level: 'warn' }] });
prisma.$on('error', (e) => logger.error('Prisma error', { message: e.message }));
prisma.$on('warn',  (e) => logger.warn('Prisma warning', { message: e.message }));
if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma;

// Redis
export const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: null,
  retryStrategy: (times) => Math.min(times * 100, 3000),
  lazyConnect: true,
});
redis.on('error', (err) => logger.error('Redis error', { err: err.message }));
redis.on('connect', () => logger.info('Redis connected'));

// Meilisearch
export const meili = new MeiliSearch({
  host: process.env.MEILI_HOST || 'http://localhost:7700',
  apiKey: process.env.MEILI_MASTER_KEY || 'masterKey',
});
export const VIDEO_INDEX = 'videos';

// ─────────────────────────────────────────────────────────
// GROQ (multi-key rotation)
// ─────────────────────────────────────────────────────────

function loadGroqKeys(): string[] {
  const keys: string[] = [];
  for (let i = 1; i <= 10; i++) {
    const key = process.env[`GROQ_API_KEY_${i}`];
    if (key) keys.push(key);
  }
  if (process.env.GROQ_API_KEY) keys.unshift(process.env.GROQ_API_KEY);
  return [...new Set(keys)];
}

const GROQ_KEYS = loadGroqKeys();
let groqKeyIndex = 0;

function getGroqClient() {
  if (GROQ_KEYS.length === 0) throw new Error('No Groq API keys configured');
  const key = GROQ_KEYS[groqKeyIndex % GROQ_KEYS.length];
  groqKeyIndex = (groqKeyIndex + 1) % GROQ_KEYS.length;
  return new Groq({ apiKey: key });
}

export async function groqComplete(prompt: string, systemPrompt?: string, maxTokens = 512): Promise<string> {
  const maxAttempts = Math.min(GROQ_KEYS.length, 3);
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const client = getGroqClient();
      const res = await client.chat.completions.create({
        model: 'llama-3.1-8b-instant',
        max_tokens: maxTokens,
        temperature: 0.7,
        messages: [
          ...(systemPrompt ? [{ role: 'system' as const, content: systemPrompt }] : []),
          { role: 'user' as const, content: prompt },
        ],
      });
      return res.choices[0]?.message?.content?.trim() || '';
    } catch (err: unknown) {
      const e = err as { status?: number; message?: string };
      if (e?.status === 429 && attempt < maxAttempts - 1) {
        logger.warn(`Groq rate limit hit, rotating key…`);
        continue;
      }
      logger.error('Groq completion failed', { err: e?.message });
      throw err;
    }
  }
  throw new Error('All Groq keys exhausted');
}

export const groqAvailable = () => GROQ_KEYS.length > 0;

// ─────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────

export async function cached<T>(key: string, ttl: number, fn: () => Promise<T>): Promise<T> {
  const hit = await redis.get(key);
  if (hit) return JSON.parse(hit) as T;
  const result = await fn();
  await redis.setex(key, ttl, JSON.stringify(result));
  return result;
}

export function makeSlug(text: string, suffix?: string): string {
  const base = slugify(text, { lower: true, strict: true, trim: true, replacement: '-' }).slice(0, 80);
  return suffix ? `${base}-${suffix}` : base;
}

export function makeVideoSlug(title: string, youtubeId: string): string {
  return makeSlug(title || 'video', youtubeId.slice(0, 6));
}

export function calculateHotScore(params: { views: number; likes: number; publishedAt: Date | null; indexedAt: Date }): number {
  const { views, likes, publishedAt, indexedAt } = params;
  const ageHours = (Date.now() - (publishedAt || indexedAt).getTime()) / 3_600_000;
  const freshness = Math.pow(0.5, ageHours / 72);
  const viewScore = Math.log10(Math.max(views, 1));
  const engagementBoost = views > 0 ? Math.min((likes / views) * 10, 2) : 0;
  return Math.max(0, Math.round((viewScore + engagementBoost) * freshness * 100 * 1000) / 1000);
}

export function batchHotScoreUpdateSql(): string {
  return `
    UPDATE videos SET hot_score = (
      (LOG(GREATEST(views, 1)) + LEAST(CASE WHEN views > 0 THEN (likes::float / views) * 10 ELSE 0 END, 2))
      * POWER(0.5, EXTRACT(EPOCH FROM (NOW() - COALESCE(published_at, indexed_at))) / 259200)
      * 100
    )
  `;
}

function serializeVideo(v: Record<string, unknown>) {
  return {
    ...v,
    id: v.id?.toString(),
    channelId: v.channelId?.toString(),
    views: v.views?.toString(),
    likes: v.likes?.toString(),
    channel: v.channel ? {
      ...(v.channel as Record<string, unknown>),
      id: (v.channel as Record<string, unknown>).id?.toString(),
    } : null,
  };
}

// ─────────────────────────────────────────────────────────
// AI SERVICES
// ─────────────────────────────────────────────────────────

const AI_SYSTEM = `You are an SEO content writer for a video discovery platform.
Write concise, accurate, and useful content. Never hallucinate facts.
Keep output within requested limits. No markdown formatting unless specified.`;

export async function generateVideoSummary(video: {
  title: string; description: string; tags: string[]; channelName: string; category: string;
}): Promise<string> {
  if (!groqAvailable()) return '';
  try {
    return await groqComplete(
      `Write a 2-sentence SEO-optimized summary for this YouTube video.\nTitle: ${video.title}\nChannel: ${video.channelName}\nCategory: ${video.category}\nTags: ${video.tags.slice(0, 10).join(', ')}\nDescription snippet: ${video.description.slice(0, 300)}\n\nSummary (2 sentences max, factual, no hype):`,
      AI_SYSTEM, 150
    );
  } catch { return ''; }
}

export async function generateSearchPageIntro(keyword: string, videoCount: number): Promise<string> {
  if (!groqAvailable()) return '';
  try {
    return await groqComplete(
      `Write a 2-sentence intro paragraph for a video search results page.\nSearch keyword: "${keyword}"\nNumber of results: ${videoCount}\n\nThe intro should explain what users will find and encourage exploration. Factual, helpful, no spam:`,
      AI_SYSTEM, 120
    );
  } catch { return ''; }
}

export async function generateRelatedKeywords(keyword: string): Promise<string[]> {
  if (!groqAvailable()) return [];
  try {
    const result = await groqComplete(
      `List 8 related search keywords for: "${keyword}"\nReturn only a JSON array of strings, no explanation. Example: ["keyword1","keyword2"]`,
      AI_SYSTEM, 200
    );
    const match = result.match(/\[.*\]/s);
    if (!match) return [];
    return (JSON.parse(match[0]) as string[]).slice(0, 8);
  } catch { return []; }
}

export async function generateVideoTags(title: string, description: string): Promise<string[]> {
  if (!groqAvailable()) return [];
  try {
    const result = await groqComplete(
      `Generate 10 relevant tags for this YouTube video.\nTitle: ${title}\nDescription: ${description.slice(0, 200)}\nReturn only a JSON array of lowercase strings:`,
      AI_SYSTEM, 150
    );
    const match = result.match(/\[.*\]/s);
    if (!match) return [];
    return (JSON.parse(match[0]) as string[]).slice(0, 10);
  } catch { return []; }
}

// ─────────────────────────────────────────────────────────
// MEILISEARCH SETUP
// ─────────────────────────────────────────────────────────

export async function setupMeiliIndex() {
  try {
    await meili.createIndex(VIDEO_INDEX, { primaryKey: 'youtubeId' }).catch(() => {});
    await meili.index(VIDEO_INDEX).updateSettings({
      searchableAttributes: ['title', 'description', 'tags', 'channelName', 'transcript', 'aiSummary'],
      filterableAttributes: ['category', 'language', 'hotScore', 'publishedAt'],
      sortableAttributes: ['hotScore', 'publishedAt', 'views'],
      rankingRules: ['words', 'typo', 'proximity', 'attribute', 'sort', 'exactness', 'hotScore:desc'],
      typoTolerance: { enabled: true, minWordSizeForTypos: { oneTypo: 4, twoTypos: 8 } },
      pagination: { maxTotalHits: 10000 },
    });
    logger.info('Meilisearch index configured');
  } catch (err) {
    logger.error('Meilisearch setup failed', { err });
  }
}

// ─────────────────────────────────────────────────────────
// INDEXER SERVICE
// ─────────────────────────────────────────────────────────

export interface VideoMeta {
  youtubeId: string; title: string; description: string; thumbnail: string;
  channelName: string; channelId: string; publishedAt: Date | null;
  duration: number | null; tags: string[]; category: string; views: number; likes: number;
}

export async function upsertVideo(meta: Partial<VideoMeta>): Promise<boolean> {
  if (!meta.youtubeId) return false;
  try {
    let channelDbId: bigint | undefined;
    if (meta.channelId && meta.channelName) {
      const channel = await prisma.channel.upsert({
        where: { youtubeChannelId: meta.channelId },
        create: { youtubeChannelId: meta.channelId, name: meta.channelName, slug: makeSlug(meta.channelName) },
        update: { name: meta.channelName },
      });
      channelDbId = channel.id;
    }

    let tags = meta.tags || [];
    if (tags.length === 0 && meta.title) tags = await generateVideoTags(meta.title, meta.description || '');

    let aiSummary = '';
    if (meta.title && meta.channelName) {
      aiSummary = await generateVideoSummary({
        title: meta.title, description: meta.description || '', tags,
        channelName: meta.channelName, category: meta.category || 'Entertainment',
      });
    }

    const hotScore = calculateHotScore({ views: meta.views || 0, likes: meta.likes || 0, publishedAt: meta.publishedAt || null, indexedAt: new Date() });
    const slug = makeVideoSlug(meta.title || 'video', meta.youtubeId);

    const video = await prisma.video.upsert({
      where: { youtubeId: meta.youtubeId },
      create: {
        youtubeId: meta.youtubeId, title: meta.title || '', slug,
        description: meta.description || '',
        thumbnail: meta.thumbnail || `https://i.ytimg.com/vi/${meta.youtubeId}/hqdefault.jpg`,
        duration: meta.duration || null, channelId: channelDbId || null,
        views: BigInt(meta.views || 0), likes: BigInt(meta.likes || 0),
        publishedAt: meta.publishedAt || null, language: meta.language || 'en',
        category: meta.category || 'Entertainment', tags, aiSummary, hotScore,
      },
      update: {
        title: meta.title || undefined, description: meta.description || undefined,
        views: meta.views ? BigInt(meta.views) : undefined, likes: meta.likes ? BigInt(meta.likes) : undefined,
        tags: tags.length > 0 ? tags : undefined, aiSummary: aiSummary || undefined,
        hotScore, category: meta.category || undefined, duration: meta.duration || undefined,
      },
      include: { channel: true },
    });

    await meili.index(VIDEO_INDEX).addDocuments([{
      youtubeId: video.youtubeId, title: video.title || '', slug: video.slug || '',
      description: video.description || '', thumbnail: video.thumbnail || '',
      duration: video.duration || 0, channelName: video.channel?.name || '',
      channelId: video.channel?.youtubeChannelId || '',
      views: Number(video.views), likes: Number(video.likes),
      category: video.category || '', tags: video.tags, language: video.language || 'en',
      hotScore: video.hotScore, publishedAt: video.publishedAt ? video.publishedAt.getTime() : 0,
      aiSummary: video.aiSummary || '',
    }]);

    return true;
  } catch (err) {
    logger.error('upsertVideo failed', { youtubeId: meta.youtubeId, err: (err as Error).message });
    return false;
  }
}

export async function getTrending(category?: string, limit = 24) {
  return prisma.video.findMany({ where: category ? { category } : {}, orderBy: { hotScore: 'desc' }, take: limit, include: { channel: true } });
}

export async function getRecent(limit = 24) {
  return prisma.video.findMany({ orderBy: { indexedAt: 'desc' }, take: limit, include: { channel: true } });
}

export async function getVideoBySlug(slug: string) {
  return prisma.video.findFirst({ where: { OR: [{ slug }, { youtubeId: slug }] }, include: { channel: true } });
}

export async function getRelatedVideos(youtubeId: string, tags: string[], limit = 12) {
  return prisma.video.findMany({
    where: { youtubeId: { not: youtubeId }, tags: { hasSome: tags.slice(0, 5) } },
    orderBy: { hotScore: 'desc' }, take: limit, include: { channel: true },
  });
}

// ─────────────────────────────────────────────────────────
// FASTIFY APP
// ─────────────────────────────────────────────────────────

const app: FastifyInstance = Fastify({ logger: false, trustProxy: true });

await app.register(cors, {
  origin: [process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000', /\.vercel\.app$/],
  credentials: true,
});

await app.register(rateLimit, {
  max: 100,
  timeWindow: '1 minute',
  errorResponseBuilder: () => ({ error: 'Rate limit exceeded' }),
});

// ─── Health ───────────────────────────────────────────────

app.get('/health', async () => ({ status: 'ok', ts: Date.now() }));

// ─── Videos (/api/videos) ─────────────────────────────────

await app.register(async (sub) => {

  // GET /api/videos/trending
  sub.get('/trending', async (req, reply) => {
    const { category, limit = '24' } = req.query as { category?: string; limit?: string };
    const videos = await cached(`trending:${category || 'all'}:${limit}`, 300, () => getTrending(category, parseInt(limit)));
    return reply.send({ videos: videos.map(serializeVideo) });
  });

  // GET /api/videos/recent
  sub.get('/recent', async (req, reply) => {
    const { limit = '24' } = req.query as { limit?: string };
    const videos = await cached(`recent:${limit}`, 120, () => getRecent(parseInt(limit)));
    return reply.send({ videos: videos.map(serializeVideo) });
  });

  // GET /api/videos/:id/related  — must be registered BEFORE /:id
  sub.get('/:id/related', async (req, reply) => {
    const { id } = req.params as { id: string };
    const video = await prisma.video.findFirst({ where: { OR: [{ youtubeId: id }, { slug: id }] } });
    if (!video) return reply.send({ videos: [] });
    const related = await cached(`related:${video.youtubeId}`, 600, () => getRelatedVideos(video.youtubeId, video.tags, 12));
    return reply.send({ videos: related.map(serializeVideo) });
  });

  // GET /api/videos/:id
  sub.get('/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    const video = await getVideoBySlug(id);
    if (!video) return reply.status(404).send({ error: 'Video not found' });
    return reply.send({ video: serializeVideo(video as unknown as Record<string, unknown>) });
  });

  // POST /api/videos/index — manually queue a video (used by worker callback or admin)
  sub.post('/index', async (req, reply) => {
    const { youtubeId } = req.body as { youtubeId?: string };
    if (!youtubeId) return reply.status(400).send({ error: 'youtubeId required' });
    // Import dynamically to avoid circular dep with worker
    const { indexQueue } = await import('./worker.js');
    await indexQueue.add(`index:${youtubeId}`, { youtubeId, source: 'manual' });
    return reply.send({ queued: true, youtubeId });
  });

}, { prefix: '/api/videos' });

// ─── Search (/api/search) ─────────────────────────────────

await app.register(async (sub) => {

  // GET /api/search?q=&page=&category=&sort=&limit=
  sub.get('/', async (req, reply) => {
    const { q, page = '1', category, sort = 'relevance', limit = '24' } = req.query as {
      q?: string; page?: string; category?: string; sort?: string; limit?: string;
    };
    if (!q?.trim()) return reply.status(400).send({ error: 'Query required' });

    const keyword = q.trim().toLowerCase();
    const pageNum = Math.max(1, parseInt(page));
    const pageSize = Math.min(48, parseInt(limit));
    const offset = (pageNum - 1) * pageSize;

    const result = await cached(`search:${keyword}:${category || ''}:${sort}:${pageNum}`, 300, async () => {
      const filter = category ? [`category = "${category}"`] : [];
      const sortArr = sort === 'hot' ? ['hotScore:desc'] : sort === 'recent' ? ['publishedAt:desc'] : sort === 'views' ? ['views:desc'] : [];
      const searchResult = await meili.index(VIDEO_INDEX).search(keyword, { limit: pageSize, offset, filter, sort: sortArr });

      let aiIntro = '';
      let relatedKeywords: string[] = [];
      if (pageNum === 1) {
        [aiIntro, relatedKeywords] = await Promise.all([
          generateSearchPageIntro(keyword, searchResult.estimatedTotalHits || 0),
          generateRelatedKeywords(keyword),
        ]);
      }

      return {
        hits: searchResult.hits,
        total: searchResult.estimatedTotalHits || 0,
        page: pageNum,
        pages: Math.ceil((searchResult.estimatedTotalHits || 0) / pageSize),
        aiIntro,
        relatedKeywords,
      };
    });

    // Upsert keyword for crawling (fire-and-forget)
    prisma.keyword.upsert({
      where: { keyword },
      create: { keyword, category: category || 'general', priority: 3 },
      update: { priority: 3 },
    }).catch(() => {});

    return reply.send(result);
  });

  // GET /api/search/suggest?q=
  sub.get('/suggest', async (req, reply) => {
    const { q } = req.query as { q?: string };
    if (!q || q.length < 2) return reply.send({ suggestions: [] });

    const suggestions = await cached(`suggest:${q.toLowerCase()}`, 600, async () => {
      const hits = await meili.index(VIDEO_INDEX).search(q, {
        limit: 8,
        attributesToRetrieve: ['title', 'youtubeId', 'slug'],
        attributesToHighlight: ['title'],
      });
      return hits.hits.map((h) => ({ title: h.title, id: h.youtubeId, slug: h.slug }));
    });

    return reply.send({ suggestions });
  });

}, { prefix: '/api/search' });

// ─── Channels (/api/channels) ─────────────────────────────

await app.register(async (sub) => {

  // GET /api/channels/:id
  sub.get('/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    const data = await cached(`channel:${id}`, 1800, async () => {
      const channel = await prisma.channel.findFirst({ where: { OR: [{ youtubeChannelId: id }, { slug: id }] } });
      if (!channel) return null;
      const videos = await prisma.video.findMany({
        where: { channelId: channel.id }, orderBy: { hotScore: 'desc' }, take: 48, include: { channel: true },
      });
      return {
        channel: { ...channel, id: channel.id.toString(), subscribers: channel.subscribers.toString() },
        videos: videos.map(serializeVideo),
      };
    });
    if (!data) return reply.status(404).send({ error: 'Channel not found' });
    return reply.send(data);
  });

  // GET /api/channels
  sub.get('/', async (req, reply) => {
    const { limit = '50', offset = '0' } = req.query as { limit?: string; offset?: string };
    const channels = await prisma.channel.findMany({
      take: Math.min(100, parseInt(limit)), skip: parseInt(offset), orderBy: { indexedAt: 'desc' },
    });
    return reply.send({
      channels: channels.map((c) => ({ ...c, id: c.id.toString(), subscribers: c.subscribers.toString() })),
    });
  });

}, { prefix: '/api/channels' });

// ─── Sitemap data (/api/sitemap) ──────────────────────────

await app.register(async (sub) => {

  sub.get('/chunks', async (_req, reply) => {
    const chunks = await prisma.sitemapChunk.findMany({ orderBy: [{ type: 'asc' }, { chunkNum: 'asc' }] });
    return reply.send({ chunks });
  });

  sub.get('/videos/:chunk', async (req, reply) => {
    const chunkNum = parseInt((req.params as { chunk: string }).chunk);
    const CHUNK_SIZE = 50000;
    const videos = await prisma.video.findMany({
      select: { youtubeId: true, slug: true, indexedAt: true },
      orderBy: { indexedAt: 'asc' },
      skip: chunkNum * CHUNK_SIZE,
      take: CHUNK_SIZE,
    });
    return reply.send({ videos, chunk: chunkNum });
  });

  sub.get('/searches/:chunk', async (req, reply) => {
    const chunkNum = parseInt((req.params as { chunk: string }).chunk);
    const CHUNK_SIZE = 50000;
    const keywords = await prisma.keyword.findMany({
      select: { keyword: true, lastCrawled: true },
      where: { crawlCount: { gt: 0 } },
      orderBy: { crawlCount: 'desc' },
      skip: chunkNum * CHUNK_SIZE,
      take: CHUNK_SIZE,
    });
    return reply.send({ keywords, chunk: chunkNum });
  });

}, { prefix: '/api/sitemap' });

// ─── Stats (/api/stats) ───────────────────────────────────

await app.register(async (sub) => {
  sub.get('/', async (_req, reply) => {
    const stats = await cached('stats:global', 300, async () => {
      const [videoCount, channelCount, keywordCount] = await Promise.all([
        prisma.video.count(), prisma.channel.count(), prisma.keyword.count(),
      ]);
      return { videoCount, channelCount, keywordCount };
    });
    return reply.send(stats);
  });
}, { prefix: '/api/stats' });

// ─────────────────────────────────────────────────────────
// BOOTSTRAP
// ─────────────────────────────────────────────────────────

async function bootstrap() {
  await setupMeiliIndex();

  const port = parseInt(process.env.PORT || '4000');
  const host = process.env.HOST || '0.0.0.0';
  await app.listen({ port, host });
  logger.info(`API server running on ${host}:${port}`);
}

bootstrap().catch((err) => {
  logger.error('Server startup failed', { err });
  process.exit(1);
});
