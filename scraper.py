import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import hashlib
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np
from collections import Counter

def scraper(url, resp, report):
    links = extract_next_links(url, resp, report)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp, report):
    """
    Extracts and processes links from the response while handling various edge cases
    and implementing the crawler requirements.
    """
    links_grabbed = []
    
    # Basic validation checks
    if not is_valid(resp.url) or resp.status != 200 or not resp.raw_response.content:
        return links_grabbed

    try:
        # Update unique pages count and subdomain tracking
        report.unique_pages += 1
        if ".ics.uci.edu" in url:
            first_index = url.index("www.") + 4 if "www." in url else url.index("//") + 2
            subdomain = url[first_index:url.index("ics.uci.edu") + 11]
            report.subdomain_count[subdomain] = report.subdomain_count.get(subdomain, 0) + 1

        # Decode content
        try:
            str_content = resp.raw_response.content.decode("utf-8", errors="replace")
        except:
            print(f"Error decoding content for {resp.raw_response.url}")
            return links_grabbed

        # Parse content
        soup = BeautifulSoup(str_content, 'html.parser')
        raw_contents = soup.get_text()

        # Check minimum content length
        if len(raw_contents) < report.min_word_threshold:
            print(f"Low page size {len(raw_contents)}")
            return links_grabbed

        # Generate and check content fingerprint
        fingerprint = np.array(simhash(url, raw_contents, report))
        for vals in report.simhash_vals:
            if similar(fingerprint, vals, report.similar_threshold):
                return links_grabbed
        report.simhash_vals.append(fingerprint)

        # Extract and process links
        for tag in soup.find_all('a', href=True):
            curr_url = tag['href']
            
            # Handle relative URLs
            if curr_url.startswith('/') and not curr_url.startswith('//'):
                if "today.uci.edu/department/information_computer_sciences/" in url:
                    domain = url[:url.index("today.uci.edu/department/information_computer_sciences") + 54]
                else:
                    domain = url[:url.index(".uci.edu") + 8]
                curr_url = domain + curr_url

            # Remove fragments
            if "#" in curr_url:
                curr_url = curr_url[:curr_url.index("#")]

            # Validate and add unique URLs
            if is_valid(curr_url) and correct_path(curr_url) and curr_url not in links_grabbed:
                links_grabbed.append(curr_url)

        print(f"number of url: {len(links_grabbed)} number of unique Pages {report.unique_pages}")
        return links_grabbed

    except Exception as e:
        print(f"Exception in extract_next_links: {str(e)}")
        return []

def is_valid(url):
    """
    Enhanced validation function with comprehensive trap checking.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # Known trap patterns
        trap_patterns = [
            r"wics\.ics\.uci\.edu/events/20",  # Calendar trap
            r"\?share=(facebook|twitter)",      # Social media shares
            r"\?action=login",                  # Login pages
            r"\.zip$", r"\.pdf$", r"\.txt$", r"\.tar\.gz$",  # File extensions
            r"\.bib$", r"\.htm$", r"\.xml$", r"\.java$",
            r"/\?afg",                          # Gallery trap
            r"/img_",                           # Image patterns
            r"action=diff&version=",            # Version diffs
            r"timeline\?from",                  # Timeline trap
            r"\?version=(?!1$)"                 # Version trap (except version=1)
        ]

        # Check against trap patterns
        for pattern in trap_patterns:
            if re.search(pattern, url):
                return False

        # Special case for doku.php
        if "doku.php" in url and "?" in url:
            return False

        # Special case for grape.ics.uci.edu
        if "grape.ics.uci.edu" in url:
            if any(pattern in url for pattern in [
                "action=diff&version=",
                "timeline?from",
                "?version=" if not url.endswith("?version=1") else None
            ]):
                return False

        # Check file extensions
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        return True

    except TypeError:
        print(f"TypeError for {url}")
        return False

def correct_path(url):
    allowed_paths = [
        r".ics.uci.edu/",
        r".cs.uci.edu/",
        r".informatics.uci.edu/",
        r".stat.uci.edu/",
        r"today.uci.edu/department/information_computer_sciences/"
    ]
    return any(path in url for path in allowed_paths)