import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import tldextract
import atexit
import storage  # your custom storage module

storage.open_shelves()                      # open shelves once per run
atexit.register(storage.close_shelves)      # ensure clean close at shutdown

stats_shelf = storage.get_stats_shelf()
words_shelf = storage.get_words_shelf()

LOW_INFO_THRESHOLD = 50
STOP_WORDS = set() 


def scraper(url, resp):
    """Main scraper function called by the crawler."""
    # 1. Validate HTTP response
    if resp.status != 200:
        return []

    # 2. Skip empty or tiny pages
    if not resp.raw_response or not resp.raw_response.content or len(resp.raw_response.content) < 50:
        return []

    # 3. Parse HTML safely
    try:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception as e:
        print(f"BeautifulSoup Error on {url}: {e}")
        return []

    # 4. Extract and clean text
    text = soup.get_text(separator=" ", strip=True)
    all_tokens = tokenize(text)
    filtered_tokens = [t for t in all_tokens if t not in STOP_WORDS]

    # 5. Update page count safely
    if "page_count" not in stats_shelf:
        stats_shelf["page_count"] = 0
    stats_shelf["page_count"] += 1

    # 6. Detect low-information pages
    if is_low_info(filtered_tokens):
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
    # Error -> malformed url
    # Exception in thread Thread-1:
    # Traceback (most recent call last):
    #   File "/usr/lib/python3.10/threading.py", line 1016, in _bootstrap_inner
    #     self.run()
    #   File "/home/thuyn18/cs-121-hw-2/crawler/worker.py", line 30, in run
    #     scraped_urls = scraper.scraper(tbd_url, resp)
    #   File "/home/thuyn18/cs-121-hw-2/scraper.py", line 56, in scraper
    #     links = extract_next_links(resp.url, soup)
    #   File "/home/thuyn18/cs-121-hw-2/scraper.py", line 104, in extract_next_links
    #     join_link = urljoin(url, link)
    #   File "/usr/lib/python3.10/urllib/parse.py", line 577, in urljoin
    #     urlparse(url, bscheme, allow_fragments)
    #   File "/usr/lib/python3.10/urllib/parse.py", line 401, in urlparse
    #     splitresult = urlsplit(url, scheme, allow_fragments)
    #   File "/usr/lib/python3.10/urllib/parse.py", line 525, in urlsplit
    #     _check_bracketed_netloc(netloc)
    #   File "/usr/lib/python3.10/urllib/parse.py", line 460, in _check_bracketed_netloc
    #     _check_bracketed_host(hostname)
    #   File "/usr/lib/python3.10/urllib/parse.py", line 469, in _check_bracketed_host
    #     ip = ipaddress.ip_address(hostname) # Throws Value Error if not IPv6 or IPv4
    #   File "/usr/lib/python3.10/ipaddress.py", line 54, in ip_address
    #     raise ValueError(f'{address!r} does not appear to be an IPv4 or IPv6 address')
    # ValueError: 'YOUR_IP' does not appear to be an IPv4 or IPv6 address
    next_links = set()
    for a in soup.find_all("a", href=True):
        link = a["href"].strip()

        # Skip obvious junk or placeholder URLs
        if not link or link.startswith("#"):
            continue
        if any(prefix in link.lower() for prefix in ["mailto:", "javascript:", "tel:"]):
            continue
        # Skip placeholder or fake hostnames like YOUR_IP, example.com, etc.
        if re.search(r"your[_-]?ip", link, re.IGNORECASE) or "example.com" in link.lower():
            continue

        # Try to safely normalize the link
        try:
            join_link = urljoin(url, link)      # make relative URLs absolute
            join_link, _ = urldefrag(join_link) # remove fragments
        except Exception:
            # Skip any malformed URL that raises during parsing
            continue

        # Validate before adding to the set
        try:
            if is_valid(join_link):
                next_links.add(join_link)
        except Exception as e:
            # Log and skip if is_valid() itself fails
            print(f"Validation error for URL {join_link}: {e}")
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


        # Avoid Trap Rules
        query = parsed.query.lower()
        path = parsed.path.lower()

        trap_keys = ["do=", "tab_", "idx=", "ns=", "image=", "ical", "calendar", "feed", "print", "session", "sid=", "sessionid=", "session_id=", "replytocom", "format=print", "action=", "option="]

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
        if ("/events/" in path and
        ("/day/" in path or "/list/" in path or re.search(r"\d{4}-\d{2}-\d{2}", path) or re.search(r"/events/category/.+/\d{4}-\d{2}", path)  # date-based URLs
        )):
            return False

        # avoid wp-json and other API endpoints
        if "/wp-json/" in url or "/xmlrpc.php" in url:
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
