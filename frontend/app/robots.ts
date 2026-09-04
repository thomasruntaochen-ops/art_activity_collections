import type { MetadataRoute } from "next";

import { SITE_URL } from "../lib/site";

// Every activity here is public, free-to-attend programming that museums want
// people to find, so the whole site is open to crawlers. The AI agents are
// listed explicitly: the goal is to be cited in AI answers, and naming them
// documents that intent so a future blanket rule doesn't quietly exclude them.
const AI_CRAWLERS = [
  "GPTBot", // OpenAI — ChatGPT training and search
  "OAI-SearchBot", // OpenAI — ChatGPT search index
  "ChatGPT-User", // OpenAI — user-initiated browsing
  "ClaudeBot", // Anthropic — Claude
  "Claude-User",
  "Claude-SearchBot",
  "PerplexityBot", // Perplexity
  "Perplexity-User",
  "Google-Extended", // Google — Gemini grounding / AI Overviews
  "Applebot", // Apple — Siri and Spotlight
  "Applebot-Extended",
  "CCBot", // Common Crawl, which seeds many AI datasets
  "Bingbot", // Bing, which backs Microsoft Copilot
  "DuckAssistBot",
  "meta-externalagent",
  "Amazonbot",
];

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      { userAgent: "*", allow: "/" },
      ...AI_CRAWLERS.map((userAgent) => ({ userAgent, allow: "/" })),
    ],
    sitemap: `${SITE_URL}/sitemap.xml`,
    host: SITE_URL,
  };
}
