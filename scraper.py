import re
from urllib.parse import urlparse, urljoin, urldefrag
import atexit

from bs4 import BeautifulSoup
import tldextract

import storage
from utils import get_logger

storage.open_shelves()
atexit.register(storage.close_shelves)

stats_shelf = storage.get_stats_shelf()
words_shelf = storage.get_words_shelf()
logger = get_logger("SCRAPER")

LOW_INFO_THRESHOLD = 50
STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
    "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
    "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did",
    "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few",
    "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
    "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
    "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if",
    "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more",
    "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
    "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own",
    "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
    "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
    "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll",
    "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
    "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while",
    "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
}



def scraper(url, resp):
    """Main scraper function called by the crawler."""
    # 1. Validate HTTP response
    if resp.status != 200:
        return []

    # 2. Skip empty or tiny pages
    if not resp.raw_response or not resp.raw_response.content or len(resp.raw_response.content) < 50:
        return []

    #skip large files
    try:
        content_length = int(resp.raw_response.headers.get('Content-Length', 0))
        if content_length > 10_000_000: #10 mb
            logger.info(f"Skipping large file: {url} ({content_length} bytes)")
            return []
    except (ValueError, TypeError):
        # Ignore if header is invalid
        pass

    # 3. Parse HTML safely
    try:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception as e:
        logger.info(f"BeautifulSoup Error on {url}: {e}")
        return []

    # 4. Extract and clean text
    text = soup.get_text(separator=" ", strip=True)
    all_tokens = tokenize(text)
    filtered_tokens = [t for t in all_tokens if t not in STOP_WORDS]

    # 5. Update page count safely
    stats_shelf["page_count"] += 1

    # 6. Detect low-information pages
    if is_low_info(filtered_tokens):
        logger.info(f"low info url: {url}")
        return []

    # 7. Analyze and store results
    analyze(url, filtered_tokens, all_tokens)

    # 8. Extract next links to crawl
    links = extract_next_links(resp.url, soup)
    return [link for link in links if is_valid(link)]


# --- Helper Functions ---
def tokenize(text):
    """Splits text into lowercase alphanumeric tokens."""
    return re.findall(r"[a-zA-Z0-9]+", text.lower())


def is_low_info(tokens):
    """Detects low-information pages based on token count and unique ratio."""
    if len(tokens) < LOW_INFO_THRESHOLD:
        return True
    unique_ratio = len(set(tokens)) / len(tokens)
    return unique_ratio < 0.2


def analyze(url, filtered_tokens, all_tokens):
    """Updates shelves with page stats: longest page, subdomains, common words."""
    n = len(all_tokens)

    # Longest page
    longest_page = stats_shelf["longest_page"]
    if n > longest_page["count"]:
        longest_page["url"] = url
        longest_page["count"] = n
        stats_shelf["longest_page"] = longest_page

    # Subdomains
    subdomain = tldextract.extract(url).subdomain or "root"
    subdomains = stats_shelf["subdomains"]
    subdomains[subdomain] = subdomains.get(subdomain, 0) + 1
    stats_shelf["subdomains"] = subdomains

    # Common words
    word_counts = {}
    for word in filtered_tokens:
        word_counts[word] = word_counts.get(word, 0) + 1
    for word, count in word_counts.items():
        words_shelf[word] = words_shelf.get(word, 0) + count


def extract_next_links(url, soup: BeautifulSoup):
    """Extracts and normalizes all valid outgoing links from a page."""

    next_links = set()
    for a in soup.find_all(["a", "area"], href=True):
        link = a["href"].strip()

        # Skip obvious junk or placeholder URLs
        if not link or link.startswith("#"):
            continue
        if any(prefix in link.lower() for prefix in ["mailto:", "javascript:", "tel:"]):
            continue

        # Skip placeholder or fake hostnames
        if re.search(r"your[_-]?ip", link, re.IGNORECASE) or "example.com" in link.lower():
            continue

        # normalize the link
        try:
            join_link = urljoin(url, link)      # make relative URLs absolute
            join_link, _ = urldefrag(join_link) # remove fragments
        except Exception as e:
            # Skip any malformed URL
            logger.info(f"malformed url: {url} , {link} , {e}")
            continue

        # Validate before adding to the set
        try:
            if is_valid(join_link):
                next_links.add(join_link)
        except Exception as e:
            # skip if is_valid() fails
            logger.info(f"invalid url: {join_link} , {e}")
            continue

    return list(next_links)


def is_valid(url):
    """Determines if a URL should be crawled."""
    # Trap here
    # https://wiki.ics.uci.edu/doku.php/projects:maint-winter-2019?tab_details=history&do=media&tab_files=files&image=security%3Avpn_settings5.png&ns=virtual_environments, status <200>, using cache ('styx.ics.uci.edu', 9001).
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        # Check domain restriction
        domain = parsed.netloc.lower()
        valid_domains = [
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
        ]
        if not any(domain.endswith(d) for d in valid_domains):
            return False

        # Reject overly long URLs (potential traps)
        if len(url) > 200:
            return False


        query = parsed.query.lower()
        path = parsed.path.lower()

        # Reject if query string is too long
        if len(query) > 100:
            return False

        # Reject if too many parameters
        if query.count('&') > 3:
            return False


        trap_keys = ["do=",
                     "tab_",
                     "idx=",
                     "ns=",
                     "image=",
                     "ical",
                     "calendar",
                     "feed",
                     "print",
                     "session",
                     "sid=",
                     "sessionid=",
                     "session_id=",
                     "replytocom",
                     "format=print",
                     "action=",
                     "option=",
                     "share=",
                     "tribe-bar-date="
                     ]

        # skip media, export/feed, dynamic session (not real page), backend parameters and other traps that have encountered
        if any(q in query for q in trap_keys):
            return False

        # block specific calendar view or export links
        # Trap here -> calendar goes to past and future date which cause forever trap
        # /events/category/volunteer-opportunity/day/2025-08-15
        # /events/category/volunteer-opportunity/day/2025-08-15/?ical=1
        # /events/category/volunteer-opportunity/day/2025-08-15/?outlook-ical=1
        # /events/category/volunteer-opportunity/list/?tribe-bar-date=2025-08-13
        # /events/category/volunteer-opportunity/list/?tribe-bar-date=2025-08-13&eventDisplay=past
        # /events/category/volunteer-opportunity/list/?tribe-bar-date=2025-08-13&ical=1
        if (
            "/events/" in path and (
                "/day/" in path or
                "/list/" in path or
                re.search(r"\d{4}-\d{2}-\d{2}", path) or   # /2025-08-15 style
                re.search(r"/events/category/.+/\d{4}-\d{2}", path)  # /fundraiser/2021-03 style
            )
        ):
            return False

        if re.search(r"/events/.*/\d{4}-\d{2}", path):
            return False

        # avoid wp-json and other API endpoints
        if "/wp-json/" in url or "/xmlrpc.php" in url:
            return False

        if '/wp-content/uploads/' in path and not path.endswith('.html'):
            return False


        # avoid repeated directory traps
        if re.search(r"(/.+)\1{2,}", path):
            return False

        # --- File extension filtering (non-HTML) ---
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower(),
        )

    except TypeError:
        print("TypeError for ", url)
        raise
