from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from loguru import logger
import pandas as pd
import argparse

class GoodReadsScraper():
    """ 
        GoodReads Scraper
    """

    def __init__(self,headless):
        """initializes the scraper by starting the engine

        Args:
            headless (boolean): checks if the scraper runs with browser open or not
        """
        self.start_engine(headless)

    def start_engine(self,headless):
        """ Initialization of the scraper

        Args:
            headless (boolean): checks if the scraper runs with browser open or not
        """

        chrome_options = Options()
        chrome_options.headless = headless
        self.driver = webdriver.Chrome(options=chrome_options)

        url = "https://www.goodreads.com/list?ref=nav_brws_lists"
        self.driver.get(url)

    def get_tags_list(self):
        """ Get all the tags from the webpage

        Returns:
            tags: list of all tags
        """
        
        content = self.driver.find_element_by_css_selector("body > div.content > div.mainContentContainer > div.mainContent > div.mainContentFloat > div.rightContainer > div.containerWithHeader.clearFloats.bigBox > div.bigBoxBody > div")
        content = content.find_elements_by_class_name("listTagsTwoColumn")

        tags = []
        for c in content:
            for element in c.find_elements_by_class_name("actionLinkLite"):
                tags.append(element.text)
                logger.info(f"Added tag {len(tags)}")
        return tags

    def get_books_urls(self,tags):
        """ Get the book urls

        Args:
            tags (list): list of tags retrieved from the main webpage

        Returns:
            books_urls: list of book urls for all tags given
        """

        base_url = "https://www.goodreads.com/list/tag/{}"

        urls_big_lists = set()
        for tag in tags:
            self.driver.get(base_url.format(tag))
            big_lists = self.driver.find_element_by_class_name("listRowsFull")
            rows = big_lists.find_elements_by_class_name("row")
            for row in rows:

                cells = row.find_elements_by_class_name("cell")
                for cell in cells:
                    url = cell.find_element_by_class_name("listImgs").find_element_by_css_selector("*").get_attribute("href")
                    urls_big_lists.add(url)
    
        books_urls = []

        # urls_big_lists = list(urls_big_lists)[:1]

        for i,url in enumerate(urls_big_lists):
            logger.info(f"Starting books list {i+1} out of {len(urls_big_lists)}")
            self.driver.get(url)
            books_list = self.driver.find_element_by_css_selector("#all_votes > table")
            books = books_list.find_elements_by_class_name("bookTitle")
            
            for j,book in enumerate(books):
                books_urls.append(book.get_attribute("href"))
            logger.info(f"Collected {len(books)} books urls")
        
        return books_urls

    def get_books_info(self,books_urls):
        """ retieve the info of the books

        Args:
            books_urls (list): list of books urls

        Returns:
            books_info: dict of book ids (same ID from GoodReads) and information about the book
        """

        books_info = {}

        for i,book_url in enumerate(books_urls):
            try:
                book_id = int(book_url.rsplit('/', 1)[-1].split('-')[0].split('.')[0])

                if book_id in books_info: break
                
                self.driver.get(book_url)
                book_name = self.driver.find_element_by_css_selector("#bookTitle").get_attribute("textContent").strip()
                book_abstract = self.driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[2]/div[4]/div[1]/div[2]/div[3]/div/span[2]").get_attribute("textContent")
                book_author = self.driver.find_element_by_css_selector("#bookAuthors > span:nth-child(2) > div > a > span").get_attribute("textContent")
                book_av_ratings = float(self.driver.find_element_by_css_selector("#bookMeta > span:nth-child(2)").get_attribute("textContent"))
                book_num_ratings = int(self.driver.find_element_by_xpath('//*[@id="bookMeta"]/a[2]').get_attribute("textContent").replace("ratings","").replace(",","").strip())

                container = self.driver.find_element_by_class_name("rightContainer")
                tags_list = container.find_element_by_class_name("stacked").find_elements_by_class_name("elementList")
                tags = set()
                for row in tags_list:
                    new_tags = row.find_element_by_class_name("left").text.split('>')
                    for tag in new_tags: tags.add(tag.strip())
                
                bookReviews = self.driver.find_element_by_id("bookReviews")
                bookReviews = bookReviews.find_elements_by_class_name("friendReviews")[:3]
                comments = {}
                for bookReview in bookReviews:
                    comment_author = bookReview.find_element_by_class_name("user").get_attribute("href")
                    comment_text = bookReview.find_element_by_xpath(".//div[1]/div[2]/span/span[2]").get_attribute("textContent").strip()
                    comments[comment_author] = comment_text

                books_info[book_id] = {
                    "name":book_name,
                    "abstract":book_abstract,
                    "author":book_author,
                    "average_rating":book_av_ratings,
                    "number_of_ratings":book_num_ratings,
                    "tags": set(tags),
                    "comments": comments
                }

                logger.info(f"Scraped {len(books_info)} books out of {len(books_urls)} books")
            except:
                logger.error("Skipped book with invalid html structure")

        return books_info

    def download(self,books_info):
        """ download the data to disk

        Args:
            books_info (dict): dict containing info of books
        """
        df = pd.DataFrame(books_info)
        df.to_csv("data.csv")

def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('headless', nargs='?', default=True, help='Run the code headless without browser')
    # args = parser.parse_args()
    # headless = args.headless

    # change to False if you want to open "chromium" browser
    headless = True

    # start the scraper
    scraper = GoodReadsScraper(headless=headless)
    
    # get all the tags available on the webpage as a list
    tags = scraper.get_tags_list()

    # get book links as a list
    books_urls = scraper.get_books_urls(tags) #[:2]
    
    # get book info as a dict
    books_info = scraper.get_books_info(books_urls)
    
    # download all the books
    scraper.download(books_info)

if __name__=="__main__":
    main()
