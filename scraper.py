import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

def scraper(url, resp):

    # scraper outline
    # -get the soup html from resp
    # -clean and tokenize it
    # -detect if low information and  200 status pages with no data and discard it?
    # -parse and store info for the questions on disk, need to log unique pages, longest page, common words, and subdomain count
    # -send soup obj to extract next links, then check links valid, then repeat for every page

    soup = BeautifulSoup(resp.raw_response.content, 'lxml')

    links = extract_next_links(resp.url, soup)
    return [link for link in links if is_valid(link)]

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
