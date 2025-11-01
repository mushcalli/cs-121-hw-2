import os
import shelve
import time

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid
from urllib.parse import urlparse


class Frontier(object):
    POLITENESS_DELAY = 0.5 # 500 ms
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = Queue() # thread-safe queue -> queue is better for multithreading 
        self.lock = RLock() # recursive lock for thread safety
        self.domain_last_access = {} # last fetch line
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.put(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            url = self.to_be_downloaded.get(timeout=2)
        except Exception:
            return None

        # Enforce politeness per domain
        domain = urlparse(url).netloc
        with self.lock:
            last_access = self.domain_last_access.get(domain, 0)
            elapsed = time.time() - last_access
            if elapsed < self.POLITENESS_DELAY:
                delay = self.POLITENESS_DELAY - elapsed
                self.logger.debug(f"Waiting {delay:.3f}s for politeness on {domain}")
                time.sleep(delay)
            self.domain_last_access[domain] = time.time()
        return url

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()

    def __del__(self):
        """close shelve when done"""
        try:
            self.save.close()
        except Exception:
            pass

