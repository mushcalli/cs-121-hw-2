#!/usr/bin/env python3
"""
CS121 HW2 – Crawl report generator
Reads the crawler's shelve databases (read-only) and prints:
  1. Total unique pages crawled
  2. Longest page (URL + word count)
  3. Subdomain counts (alphabetical)
  4. 50 most common words (excluding stopwords)
"""

import shelve
from collections import Counter

# --- Open shelves in READ-ONLY mode ---
stats = shelve.open("crawler_stats.db", flag="r")
words = shelve.open("crawler_words.db", flag="r")

print("=" * 70)
print("🧭  CS121 CRAWLER REPORT")
print("=" * 70)

# 1️⃣ Total unique pages crawled
page_count = stats.get("page_count", 0)
print(f"\n📄 Total unique pages crawled: {page_count}")

# 2️⃣ Longest page (URL + word count)
longest_page = stats.get("longest_page", {"url": None, "count": 0})
print(f"\n📏 Longest page:")
print(f"   URL: {longest_page.get('url', 'N/A')}")
print(f"   Word count: {longest_page.get('count', 0)}")

# 3️⃣ Subdomain counts (sorted alphabetically)
subdomains = stats.get("subdomains", {})
print("\n🌐 Subdomain counts:")
for sub, count in sorted(subdomains.items()):
    print(f"   {sub}.ics.uci.edu, {count}")

# 4️⃣ Top 50 most common words (sorted by frequency)
print("\n🔤 Top 50 most common words:")
top_words = Counter(words).most_common(50)
for word, count in top_words:
    print(f"   {word:<20} {count}")

print("=" * 70)
print("✅ Report generation complete.")
print("=" * 70)

# --- Close shelves ---
stats.close()
words.close()
