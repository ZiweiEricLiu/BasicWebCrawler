import atexit
import logging

import sys

from corpus import Corpus
from crawler import Crawler
from frontier import Frontier

if __name__ == "__main__":
    # Configures basic logging
    logging.basicConfig(format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    # Instantiates frontier and loads the last state if exists
    frontier = Frontier()
    frontier.load_frontier()

    # Instantiates corpus object with the given cmd arg
    corpus = Corpus(sys.argv[1])

    # Registers a shutdown hook to save frontier state upon unexpected shutdown
    atexit.register(frontier.save_frontier)

    # Instantiates a crawler object and starts crawling
    crawler = Crawler(frontier, corpus)
    crawler.start_crawling()

    # Write visited subdomains to file
    vs = open("visited_subdomains.txt", 'w', encoding='utf-8')
    crawler.printVisitedSubdomains(vs)
    vs.close()

    # Write max out links to file
    max_links = open("max_out_links.txt", 'w', encoding='utf-8')
    crawler.printMaxOutLink(max_links)
    max_links.close()

    # Write longest page to file
    longest_page = open("longest_page.txt", 'w', encoding='utf-8')
    crawler.printLongestPage(longest_page)
    longest_page.close()

    # Write most common words to file
    common_words = open("most_common_words.txt", 'w', encoding='utf-8')
    crawler.printCommonWords(common_words)
    common_words.close()