from threading import Thread, Lock

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from .sim import exact_hash, shingles, similarity

# near similarity threshold 
SIMILARITY_THRESHOLD = 0.9

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # add hash and shingles for similarity detection
        self.hashes = set()
        self.shingles = {}
        self.lock = Lock()
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)

            # skip invalid response
            if resp is None or resp.status >= 400 or resp.raw_response is None:
                self.logger.warning(f"Invalid or empty response for {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # detection here
            try:
                content = resp.raw_response.content.decode("utf-8", errors="ignore")
            except Exception:
                content = str(resp.raw_response.content)

            # skip very small pages
            if len(content) < 100:
                self.frontier.mark_url_complete(tbd_url)
                continue

            page_hashes = exact_hash(content)
            with self.lock: # multithreading
                if page_hashes in self.hashes:
                    self.logger.info(f"[Duplicate] Skipping exact duplicate: {tbd_url}")
                    self.frontier.mark_url_complete(tbd_url)
                    continue
                self.hashes.add(page_hashes)
                page_shingles = shingles(content)
                # check near similarity here
                is_near_duplicate = False
                for other_url, other_shingles in self.shingles.items():
                    # compute similarity and compare with threshold
                    # if nearly similar, complete
                    sim = similarity(page_shingles, other_shingles)
                    if sim >= SIMILARITY_THRESHOLD:
                        self.logger.info(f"[Near-duplicate] {tbd_url} ≈ {other_url} (similarity={sim:.2f})")
                        is_near_duplicate = True
                        break
        
                if is_near_duplicate:
                    self.frontier.mark_url_complete(tbd_url)
                    continue
    
                # Store shingles for this page
                self.shingles[tbd_url] = page_shingles
    
                # end detection
            
            
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            # time.sleep(self.config.time_delay) -> handled in frontier --> check 
