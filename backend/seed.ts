/**
 * seed.ts — run once to populate the keywords table
 * npx tsx seed.ts
 */
import 'dotenv/config';
import { prisma, logger } from './server.js';

const SEED_KEYWORDS: Array<{ keyword: string; category: string; priority: number }> = [
  // Alphabet crawl
  ...Array.from('abcdefghijklmnopqrstuvwxyz').map((l) => ({ keyword: l, category: 'general', priority: 1 })),
  // Music
  { keyword: 'music',               category: 'music',         priority: 10 },
  { keyword: 'new music 2024',      category: 'music',         priority: 9  },
  { keyword: 'top songs',           category: 'music',         priority: 9  },
  { keyword: 'hip hop',             category: 'music',         priority: 8  },
  { keyword: 'pop music',           category: 'music',         priority: 8  },
  { keyword: 'rnb',                 category: 'music',         priority: 8  },
  { keyword: 'afrobeats',           category: 'music',         priority: 8  },
  { keyword: 'amapiano',            category: 'music',         priority: 7  },
  { keyword: 'music video',         category: 'music',         priority: 9  },
  { keyword: 'official music video',category: 'music',         priority: 9  },
  { keyword: 'remix',               category: 'music',         priority: 7  },
  { keyword: 'live performance',    category: 'music',         priority: 7  },
  { keyword: 'concert',             category: 'music',         priority: 7  },
  // Sports
  { keyword: 'football',            category: 'sports',        priority: 10 },
  { keyword: 'soccer highlights',   category: 'sports',        priority: 9  },
  { keyword: 'basketball',          category: 'sports',        priority: 9  },
  { keyword: 'nba highlights',      category: 'sports',        priority: 9  },
  { keyword: 'nfl highlights',      category: 'sports',        priority: 9  },
  { keyword: 'premier league',      category: 'sports',        priority: 9  },
  { keyword: 'champions league',    category: 'sports',        priority: 9  },
  { keyword: 'goals',               category: 'sports',        priority: 8  },
  { keyword: 'match highlights',    category: 'sports',        priority: 8  },
  { keyword: 'boxing',              category: 'sports',        priority: 7  },
  { keyword: 'mma',                 category: 'sports',        priority: 7  },
  { keyword: 'tennis',              category: 'sports',        priority: 7  },
  { keyword: 'cricket',             category: 'sports',        priority: 7  },
  // Entertainment
  { keyword: 'movie trailer',       category: 'entertainment', priority: 10 },
  { keyword: 'trailer 2024',        category: 'entertainment', priority: 9  },
  { keyword: 'comedy',              category: 'entertainment', priority: 8  },
  { keyword: 'funny videos',        category: 'entertainment', priority: 8  },
  { keyword: 'viral video',         category: 'entertainment', priority: 9  },
  { keyword: 'nollywood',           category: 'entertainment', priority: 8  },
  // News
  { keyword: 'news today',          category: 'news',          priority: 10 },
  { keyword: 'breaking news',       category: 'news',          priority: 10 },
  { keyword: 'world news',          category: 'news',          priority: 9  },
  { keyword: 'politics',            category: 'news',          priority: 8  },
  // Tech
  { keyword: 'technology',          category: 'tech',          priority: 8  },
  { keyword: 'review 2024',         category: 'tech',          priority: 8  },
  { keyword: 'iphone',              category: 'tech',          priority: 7  },
  { keyword: 'artificial intelligence', category: 'tech',      priority: 8  },
  { keyword: 'tutorial',            category: 'tech',          priority: 8  },
  // Gaming
  { keyword: 'gaming',              category: 'gaming',        priority: 9  },
  { keyword: 'gameplay',            category: 'gaming',        priority: 8  },
  { keyword: 'fortnite',            category: 'gaming',        priority: 8  },
  { keyword: 'minecraft',           category: 'gaming',        priority: 8  },
  { keyword: 'game review',         category: 'gaming',        priority: 7  },
  // Education
  { keyword: 'how to',              category: 'education',     priority: 9  },
  { keyword: 'learn',               category: 'education',     priority: 8  },
  { keyword: 'explained',           category: 'education',     priority: 7  },
  { keyword: 'documentary',         category: 'education',     priority: 7  },
  // Lifestyle
  { keyword: 'vlog',                category: 'lifestyle',     priority: 8  },
  { keyword: 'cooking',             category: 'lifestyle',     priority: 7  },
  { keyword: 'travel',              category: 'lifestyle',     priority: 7  },
  { keyword: 'fitness',             category: 'lifestyle',     priority: 7  },
];

async function seed() {
  logger.info(`Seeding ${SEED_KEYWORDS.length} keywords…`);
  for (const kw of SEED_KEYWORDS) {
    await prisma.keyword.upsert({
      where:  { keyword: kw.keyword },
      create: kw,
      update: { priority: kw.priority },
    });
  }
  logger.info('Seed complete');
  await prisma.$disconnect();
}

seed().catch((e) => {
  logger.error('Seed failed', { err: e });
  process.exit(1);
});
