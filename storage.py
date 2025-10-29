import shelve

_stats_shelf = None
_words_shelf = None

def open_shelves():
    global _stats_shelf, _words_shelf

    if _stats_shelf is None:
        _stats_shelf = shelve.open("crawler_stats.db")
        if 'longest_page' not in _stats_shelf:
            _stats_shelf['longest_page'] = {'url': 'None', 'count': 0}
        if 'subdomains' not in _stats_shelf:
            _stats_shelf['subdomains'] = {}
        if 'page_count' not in _stats_shelf:
            _stats_shelf['page_count'] = 0
        if 'valid_page_count' not in _stats_shelf:
            _stats_shelf['valid_page_count'] = 0

    if _words_shelf is None:
         _words_shelf = shelve.open("crawler_words.db")

def close_shelves():
    global _stats_shelf, _words_shelf
    if _stats_shelf is not None:
        _stats_shelf.close()
        _stats_shelf = None
    if _words_shelf is not None:
        _words_shelf.close()
        _words_shelf = None

def get_stats_shelf():
    return _stats_shelf

def get_words_shelf():
    return _words_shelf