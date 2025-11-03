import shelve
from collections import Counter

stats = shelve.open("crawler_stats.db", flag="r")
words = shelve.open("crawler_words.db", flag="r")


# Total unique pages crawled
page_count = stats.get("page_count", 0)
print(f"\n Total unique pages crawled: {page_count}")

# Longest page (URL + word count)
longest_page = stats.get("longest_page", {"url": None, "count": 0})
print(f"\n Longest page:")
print(f"   URL: {longest_page.get('url', 'N/A')}")
print(f"   Word count: {longest_page.get('count', 0)}")

# Subdomain counts (sorted alphabetically)
subdomains = stats.get("subdomains", {})
print("\n Subdomain counts:")
for sub, count in sorted(subdomains.items()):
    print(f"   {sub}.ics.uci.edu, {count}")

# Top 50 most common words (sorted by frequency)
print("\n Top 50 most common words:")
top_words = Counter(words).most_common(50)
for word, count in top_words:
    print(f"   {word:<20} {count}")

print("=" * 70)

# --- Close shelves ---
stats.close()
words.close()
