import re
import time
import hashlib
from urllib.parse import urlparse, urljoin, urlsplit
from collections import Counter, defaultdict
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords

# nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

# global containers 
unique_urls = set()
word_counter = Counter()
page_word_counts = {}
subdomain_counts = defaultdict(int)
content_hashes = set()  #  detecting duplicate content
visited_patterns = defaultdict(int)  #  detecting pattern-based traps

MAX_PATTERN_URLS = 30  # max number of similar URLs to crawl
MAX_PATH_DEPTH = 8    # max depth for any path segment

ALLOWED_PATHS = [
    re.compile(r".*\.ics\.uci\.edu/.*"), 
    re.compile(r".*\.cs\.uci\.edu/.*"), 
    re.compile(r".*\.informatics\.uci\.edu/.*"), 
    re.compile(r".*\.stat\.uci\.edu/.*"), 
    re.compile(r".*today\.uci\.edu/department/information_computer_sciences/.*")
]

TRAP_PATTERNS = [
    re.compile(pattern) for pattern in [
        r"wics\.ics\.uci\.edu/events/20",
        r"\?share=(facebook|twitter)",
        r"\?action=login",
        r"action=diff&version=",
        r"timeline\?from",
        r"\?version=(?!1$)",
        r"/calendar/",
        r"/archive/",
        r"/ml/datasets.php",
        r"/print/",
        r"/rss/",
        r"/feed/",
        r"/tags/",
        r"/404",
        r"/auth", 
        r"/~eppstein/pix/", 
        r"/~eppstein/pubs",
        r"/category/page/\d+"
    ]
]

def is_duplicate_content(content):
    content_hash = hashlib.md5(content).hexdigest()
    if content_hash in content_hashes:
        return True
    content_hashes.add(content_hash)
    return False

def scraper(url, resp):
    # log or track failed requests
    if resp.status in [404, 604]:
        print(f"Skipping {url} - Status code: {resp.status}")
        return []
    
    # only successful responses
    if resp.status != 200:
        return []

    # skip if content is duplicate
    if resp.raw_response and is_duplicate_content(resp.raw_response.content):
        return []

    # track unique URLs without fragments
    add_unique_url(url)
    # count words on the page 
    count_words(resp)
    # extract and filter links based on allowed paths
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]

    time.sleep(0.5) 
    return valid_links

def add_unique_url(url):
    base_url = urlsplit(url)._replace(fragment='', query='').geturl()  # remove query parameters
    unique_urls.add(base_url)
    track_subdomain(base_url)

def track_subdomain(url):
    parsed_url = urlparse(url)
    if 'uci.edu' in parsed_url.netloc:
        subdomain = parsed_url.netloc
        subdomain_counts[subdomain] += 1

def extract_next_links(url, resp):
    links = []
    if resp.status == 200 and resp.raw_response:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            link = urljoin(url, a_tag['href'])
            links.append(link)
    return links

def is_valid(url):
    try:
        parsed = urlparse(url)

        # scheme validation
        if parsed.scheme not in {"http", "https"}:
            return False

        # check path depth to avoid deep recursion traps
        path_segments = parsed.path.split('/')
        if len(path_segments) > 8:
            return False

        # check for known trap patterns
        for pattern in TRAP_PATTERNS:
            if pattern.search(url):
                return False

        # more trap conditions
        if url.startswith("https://wics.ics.uci.edu/events/") \
                or url.endswith("?share=facebook") \
                or url.endswith("?share=twitter") \
                or url.endswith("?action=login") \
                or url.endswith(".zip") \
                or url.endswith(".pdf") \
                or url.endswith("txt") \
                or url.endswith("tar.gz") \
                or url.endswith(".bib") \
                or url.endswith(".htm") \
                or url.endswith(".xml") \
                or url.endswith(".bam") \
                or url.endswith(".java"):
            return False

        if url.startswith("http://www.ics.uci.edu/~eppstein/pix/"):
            return False

        # traps for 'wics' subdomain patterns
        if "wics" in url and "/?afg" in url and not url.endswith("page_id=1"):
            return False
        elif "wics" in url and "/img_" in url:
            return False

        # trap for "doku.php"
        if "doku.php" in url:
            return False
    
        # no information
        if "sli.ics.uci.edu/Classes" in url:
            return False

        # trap for "grape.ics.uci.edu" with specific patterns
        if "grape.ics.uci.edu" in url and (
            "action=diff&version=" in url or
            "timeline?from" in url or
            ("?version=" in url and not url.endswith("?version=1"))
        ):
            return False

        # detect and limit repetitive patterns in visited patterns to avoid traps
        path_pattern = re.sub(r'\d+', 'N', parsed.path)
        visited_patterns[path_pattern] += 1
        if visited_patterns[path_pattern] > MAX_PATTERN_URLS:
            return False

        # check if URL matches allowed paths
        if not any(pattern.match(url) for pattern in ALLOWED_PATHS):
            return False

        # file extension exclusion to avoid non-crawlable files
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|mpg|img|war|apk|py|ppsx|pps)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise

def count_words(resp):
    if resp.status == 200 and resp.raw_response:
        try:
            soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
            # remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # get text and normalize whitespace
            text = ' '.join(soup.stripped_strings)
            words = [
                word.lower() for word in re.findall(r'\b\w+\b', text)
                if word.lower() not in stop_words and len(word) > 1
            ]
            page_word_counts[resp.url] = len(words)
            word_counter.update(words)
        except Exception as e:
            print(f"Error counting words on page {resp.url}: {str(e)}")

# result reporting
def get_unique_page_count():
    return len(unique_urls)

def get_longest_page():
    if page_word_counts:
        longest_page = max(page_word_counts, key=page_word_counts.get)
        return longest_page, page_word_counts[longest_page]
    return None, 0

def get_most_common_words(n=50):
    return word_counter.most_common(n)

def get_subdomain_counts():
    return sorted(subdomain_counts.items())

def write_stats_to_file(output_file="scraper_stats.txt"):
    with open(output_file, 'w', encoding='utf-8') as f:
        # 1. Unique pages count
        unique_count = get_unique_page_count()
        f.write("1. Number of unique pages found:\n")
        f.write(f"{unique_count}\n\n")

        # 2. Longest page
        longest_url, word_count = get_longest_page()
        f.write("2. Longest page by word count:\n")
        f.write(f"URL: {longest_url}\n")
        f.write(f"Word count: {word_count}\n\n")

        # 3. 50 most common words
        f.write("3. 50 most common words:\n")
        common_words = get_most_common_words(50)
        for word, count in common_words:
            f.write(f"{word}: {count}\n")
        f.write("\n")

        # 4. Subdomain statistics
        f.write("4. Subdomains found:\n")
        subdomains = get_subdomain_counts()
        for subdomain, count in subdomains:
            f.write(f"{subdomain}, {count}\n")