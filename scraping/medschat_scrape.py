import re
import requests
import urlparse
import dateutil
import multiprocessing
import threading
from pymongo import MongoClient
from bs4 import BeautifulSoup

class MedschatScraper(object):
    def __init__(self, base_path='http://www.medschat.com',
            db_name='dsi_capstone', coll_name='posts'):
        self.base_path = base_path
        self.mongo_client = MongoClient()
        self.db = self.mongo_client.get_database(db_name)
        self.coll = self.db.get_collection(coll_name)

    def parse_discuss(self, path):
        '''
        parse_discuss stores the results from a topic forum, e.g.
        www.medschat.com/Discuss/Effexor

        It creates a thread for each page, which calls parse_discuss_page.
        '''
        threads = []
        response = requests.get(path)
        soup = BeautifulSoup(response.content)
        # Get the number of pages in the topic.
        pages_elements = soup.select('form.action_heading.noprint')
        if pages_elements:
            num_pages = int(pages_elements[0].find('a', attrs={'class': None}).get_text())
        else:
            num_pages = 1
        for i in xrange(num_pages):
            url = path + '/{}/'.format(i+1)
            thread = threading.Thread(
                target=self.parse_discuss_page,
                args=[url])
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def parse_discuss_page(self, path):
        '''
        parse_discuss_page stores the results from a topic forum page, e.g.
        www.medschat.com/Discuss/Vyvanse/2/

        The results are stored in MongoDB.
        '''
        threads = []
        response = requests.get(path)
        soup = BeautifulSoup(response.content)
        items = soup.select('div.list_item')
        for item in items:
            path = item.find_next('a')['href']
            url = urlparse.urljoin(self.base_path, path)
            thread = threading.Thread(target=self.parse_thread, args=[url])
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def parse_thread(self, path):
        '''
        parse_thread takes in a url to a thread and returns the post contents
        from all pages of that thread, by calling parse_thread_page on each of
        the pages.

        Spawns a thread for each page in the thread, having each thread run
        parse_thread_page.

        Args:
            path: The URL path to the thread.

        Returns:
            A list of dictionary objects containing the post content and
            metadata for all posts from all pages.
        '''
        response = requests.get(path)
        soup = BeautifulSoup(response.content)

        # Get the number of pages in the thread.
        pages_elements = soup.select('form.action_heading.noprint')
        if pages_elements:
            num_pages = int(pages_elements[0].find('a', attrs={'class': None}).get_text())
        else:
            num_pages = 1

        threads = []
        for i in xrange(num_pages):
            if i == 0:
                include_op = True
            else:
                include_op = False
            url = path.replace('.htm', '_p{}.htm'.format(i+1))
            thread = threading.Thread(
                    target=self.parse_thread_page,
                    args=[url],
                    kwargs={'include_op': include_op})
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def parse_thread_page(self, path, include_op=True, write_to_db=True):
        '''
        parse_thread_page returns a list of the posts from a discussion page.

        Args:
            path: The URL path of the thread page.
            include_op: Specifies whether the original post in the forum should
                be included in the results (defaults to True).
            write_to_db: If True, write to MongoDB rather than returning.
        Returns:
            A list of dictionary objects containing post content and metadata
            for all posts on the page.
        '''
        out = []
        response = requests.get(path)
        soup = BeautifulSoup(response.content)
        posts = soup.select('div.quote_item')

        # Extract drug name.
        drug_name = soup.select('div.small.breadcrumb')[0].select('span')[5].get_text()

        # Extract forum ID.
        forum_id = re.match('.*?([0-9]+)[^-]*.htm', path).group(1)

        for i, post in enumerate(posts):
            try:
                post_info = {}
                if i == 0 and not include_op:
                    continue

                # Annotate drug name and forum ID.
                post_info['drug_name'] = drug_name
                post_info['forum_id'] = forum_id

                # Extract upvote/downvote information.
                if i != 0:
                    ratings = post.select('span.votes_number')
                    post_info['upvotes'] = int(ratings[0].get_text())
                    post_info['downvotes'] = int(ratings[1].get_text())

                # Annotate with original page's URL.
                post_info['page_url'] = path

                # Extract date information.
                post_info['ts'] = dateutil.parser.parse(
                    post.findPreviousSiblings()[1].find('time').get_text())

                # Extract post number.
                if i == 0:
                    post_info['post_no'] = 0
                else:
                    post_info['post_no'] = int(post.findPreviousSiblings()[4].get_text())

                # Extract username.
                post_info['username'] = post.findPreviousSiblings()[3].get_text()

                # Remove noprint information from text before extracting content.
                [noprint.extract() for noprint in post.select('div.noprint')]

                # Extract post content (message).
                post_info['content'] = post.get_text()
                out.append(post_info)
            except:
                print path, i

        if write_to_db:
            self.coll.insert_many(out)
        else:
            return out
