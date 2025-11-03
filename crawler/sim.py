import hashlib

# Find exact using hash
def exact_hash(text):
    '''
    Use SHA-1 to compute hash values of the content at two different points in 
    time or locations and then compare the two hash values. If they are the same, the pages are identical
    SHA-1 hash is a unique 40-digit hexadecimal string, or "message digest" 
    '''
    return hashlib.sha1(text.encode('utf-8')).hexdigest()

# Find near similar using fingerprint
# Step 1: Tokenize text
# Step 2: Generate k-shingles (word windows) -> sequence of k consecutive tokens or local structure 
# -> example: 1. "uc irvine is"
# 2. "irvine is a"
# 3. "is a public"
# 4. "a public research"
# 5. "public research university"
# 6. "research university in"
# 7. "university in california"
# Step 3: Compare sets using similarity -> on lecture 11 slide 37 -> if sim > 0.9, they are nearly identical
def shingles(text, k=5):
    '''
    Tokenize and return a set of k-shingles from text (html content)
    Set k = 5 because I think it is a balance -> if set small, it will be too small (recall) and may capture extremely 
    small changes that is not crucial; else, it would be too large (precision) and might miss the small changes
    '''
    tokens = text.lower().split() # tokenize
    # base case when tokens have len smaller than k -> very short text like "hello world"
    if len(tokens) < k:
        return set([" ".join(tokens)])
    shingles_set = set()
    for i in range(len(tokens) - k + 1): 
        shingle = " ".join(tokens[i:i+k])
        shingles_set.add(shingle)
    return shingles_set

def similarity(set1, set2):
    '''
    calculate similarity by taking fraction of the intersection over the union of the fingerprint sets of 1 and 2
    '''
    inter = 0
    for item in set1:
        if item in set2:
            inter += 1
    union = len(set1) + len(set2) - inter
    return inter / union if union else 0


