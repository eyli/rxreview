import requests
import urlparse
from bs4 import BeautifulSoup

class MedschatScraper(object):
    def __init__(self, base_path='http://www.medschat.com'):
        self.base_path = base_path

    def parse_discuss_page(self, path, include_op=True):
        '''
        parse_discuss_page returns a list of the posts from a discussion page.

        Args:
            path: The relative path of the page with search results.
            include_op: Specifies whether the original post in the forum should
                be included in the results (defaults to True).
        Returns:
            A list of strings containing the content of each post on the
            search results page.
        '''
        out = []
        response = requests.get(urlparse.urljoin(self.base_path, path))
        soup = BeautifulSoup(response.content)
        posts = soup.select('div.quote_item')
        for i, post in enumerate(posts):
            if i == 0 and not include_op:
                continue
            [noprint.extract() for noprint in post.select('div.noprint')]
            out.append(post.get_text())
        action_buttons = soup.select('a.action_button')
        next_page = None
        for button in action_buttons:
            if button['title'] == 'Next Page':
                next_page = button['href']
        if next_page:
            out += parse_search_results(urlparse.urljoin(base_url, next_page),
                                        include_op=False)
        return out
