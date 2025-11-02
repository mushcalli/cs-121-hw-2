import storage
import atexit

storage.open_shelves()
atexit.register(storage.close_shelves)

stats_shelf = storage.get_stats_shelf()
words_shelf = storage.get_words_shelf()

print(words_shelf)
print(stats_shelf)