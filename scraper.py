import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import shelve
import tldextract

def init_shelves():
    '''initializes the shelves the first time scraper.py is launched'''
    stats_shelf = shelve.open('stats_shelf.db')
    words_shelf = shelve.open('words_shelf.db')

    if 'page_count' not in stats_shelf:
        stats_shelf['page_count'] = 0

    if 'longest_page' not in stats_shelf:
        stats_shelf['longest_page'] = {'url': 'None', 'count': 0}

    if 'subdomains' not in stats_shelf:
        stats_shelf['subdomains'] = dict()

    return stats_shelf, words_shelf

stats_shelf, words_shelf = init_shelves()
STOP_WORDS = set([])

def scraper(url, resp):

    # scraper outline
    # -clean and tokenize it
    # -detect if low information and 200 status pages with no data and discard it?
    # -parse and store info for the questions on disk, need to log unique page count, longest page, common words, and subdomain count
    # -send soup obj to extract next links, then check links valid, then repeat for every page

    #check http status
    if resp.status != 200:
        return []

    #check for large size

    #get the soup html from resp
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    except Exception as e:
        print(f"BeautifulSoup Error on {url}: {e}")
        return []

    #clean get html and tokenize it
    text = soup.get_text(separator = " ", strip = True)
    all_tokens = tokenize(text)

    #filter tokens
    filtered_tokens = [token for token in all_tokens if token not in STOP_WORDS]

    # add to page_count before low info check
    stats_shelf['page_count'] += 1

    #avoid low info pages somehow?
    if (is_low_info(filtered_tokens)):
        return []

    # -parse and store info for the questions on disk, need to log unique pages, longest page, common words, and subdomain count
    analyze(url, filtered_tokens, all_tokens)

    # -send soup obj to extract next links, then check links valid, then repeat for every page
    links = extract_next_links(resp.url, soup)
    return [link for link in links if is_valid(link)]

def tokenize(text):
    '''converts a string of text to tokens'''
    pass

def is_low_info(tokens):
    '''determines if the page is low info based on low word count and low unique word ratio'''
    pass

def analyze(url, filtered_tokens, all_tokens):
    '''updates shelves with info from the page for the required report '''
    n = len(all_tokens)

    #longest_page
    longest_page_dict = stats_shelf['longest_page']
    if n > longest_page_dict['count']:
        longest_page_dict['url'] = url
        longest_page_dict['count'] = n
    stats_shelf['longest_page'] = longest_page_dict

    #subdomains
    subdomain = tldextract.extract(url).subdomain
    subdomain_dict = stats_shelf['subdomains']
    subdomain_dict[subdomain] = subdomain_dict.get(subdomain, 0) + 1
    stats_shelf['subdomains'] = subdomain_dict

    #common words
    word_counts = {}
    for word in filtered_tokens:
        word_counts[word] = word_counts.get(word, 0) + 1

    for word, count  in word_counts.items():
        words_shelf[word] = words_shelf.get(word, 0) + count


def extract_next_links(url, soup : BeautifulSoup):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    #updated function header to use soup
    #I think you shouldn't need the resp status or error because scraper will handle that
    #url will be resp.url,

    return list()

def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
