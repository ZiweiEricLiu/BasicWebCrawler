import logging
import re
from urllib.parse import urlparse
from lxml import html, etree
from collections import deque, defaultdict

logger = logging.getLogger(__name__)

stop_word_file = open('stop_words.txt', 'r')
STOP_WORDS = stop_word_file.read().split()

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.url_history = deque(maxlen=10)
        self.max_out_links_url = None
        self.max_out_links = 0
        self.visited_subdomains = defaultdict(int)
        self.longest_page_url = None
        self.longest_page_word_count = 0
        self.word_counts = defaultdict(int)

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """


        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            # Find the page with the most valid out links
            number_of_valid_links = 0

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    number_of_valid_links += 1
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

            # Find the page with the most valid out links
            if number_of_valid_links > self.max_out_links:
                self.max_out_links = number_of_valid_links
                self.max_out_links_url = url

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        url = url_data["final_url"] if url_data["is_redirected"] else url_data["url"]
        self.url_history.append(url)

        if url_data["content"] and url_data["http_code"] < 400:
            try:
                utf8_parser = html.HTMLParser(encoding='utf-8')
                dochtml = html.fromstring(url_data["content"], parser=utf8_parser)
                dochtml.make_links_absolute(base_url=url)
                for el, attr, link, pos in dochtml.iterlinks():
                    outputLinks.append(link)

                # Keep track of the subdomains that it visited
                self.visited_subdomains[urlparse(url).hostname] += 1

                # Write downloaded urls to file
                downloaded = open("downloaded_urls.txt", 'a', encoding='utf-8')
                downloaded.write(url+'\n')
                downloaded.close()

                # Keep track of the longest page in terms of number of words
                words = dochtml.text_content().split()
                if len(words) > self.longest_page_word_count:
                    self.longest_page_url = url
                    self.longest_page_word_count = len(words)

                # Keep track of the most common words (stop words ignored)
                for w in words:
                    w = w.lower()
                    if w not in STOP_WORDS and re.match("^[a-zA-Z]*'?[a-zA-Z]*$", w):
                        self.word_counts[w] += 1

            except:
                print(f'Skipping invalid HTML from URL {url}')

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        trap_urls = open("trap_urls.txt", 'a', encoding='utf-8')

        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # Checking long urls
        if len(url) > 300:
            trap_urls.write(url+'\n')
            trap_urls.write("Reason: Too Long"+'\n\n')
            return False

        # Checking continuously repeating sub-directories
        if re.match(r"^.*?(\/.+?\/).*?\1$|^.*?\/(.+?\/)\2.*$", parsed.path.lower()):
            trap_urls.write(url+'\n')
            trap_urls.write("Reason: Repeating sub-directories"+'\n\n')
            return False
        
        # History traps detection
        # Checking if crawler has been trapped in a directory by looking at the url_history
        # This also handles the problem of query because same pages with different query should be in the same directory
        similar_counter = 0
        for pre_url in self.url_history:
            if parsed.path.lower() != '/' and urlparse(pre_url).path.lower() == parsed.path.lower():
                similar_counter += 1
        if similar_counter >= 5:
            trap_urls.write(url+'\n')
            trap_urls.write("Reason: Staying in same directory for too long"+'\n\n')
            return False

        # Checking if the url is actually the same as a url that has been fetched except for fragmentation.
        if '#' in url and url.split('#', 1)[0] in self.frontier.urls_set:
            trap_urls.write(url+'\n')
            trap_urls.write("Reason: Same URL but diff frag"+'\n\n')
            return False

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

        trap_urls.close()

    # Helper function to write max out link info to file
    def printMaxOutLink(self, file):
        file.write(f"URL with most valid out links: {self.max_out_links_url}\nNumber of valid out links: {self.max_out_links}")

    # Helper function to write visited subdomains to file
    def printVisitedSubdomains(self, file):
        file.write("Visited subdomains\n\n")
        for k,v in self.visited_subdomains.items():
            file.write(f"{k}: {v}\n")

    # Helper function to write the longest page info to file
    def printLongestPage(self, file):
        file.write(f"URL with most words: {self.longest_page_url}\nNumber of words: {self.longest_page_word_count}")

    # Helper function to write 50 most common words to file
    def printCommonWords(self, file):
        file.write("50 Most Common Words\n\n")
        count = 0
        for k,v in sorted(self.word_counts.items(), key=lambda i: -i[1]):
            file.write(f"{k}: {v}\n")
            count += 1
            if count == 50:
                break