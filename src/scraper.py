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
            map: dictionary with keys as the book url and values as list of big_lists urls (used mainly for validation or finding relevance of our retrieval results)
        """

        # first get the big lists urls
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
            
            logger.info(f"Collected all links for tag {tag}")

        # then get the actual book urls from each big list url
        books_urls = []
        map = {}
        # urls_big_lists = list(urls_big_lists)[:1]

        for i,url in enumerate(urls_big_lists):
            logger.info(f"Starting books list {i+1} out of {len(urls_big_lists)}")
            self.driver.get(url)
            books_list = self.driver.find_element_by_css_selector("#all_votes > table")
            books = books_list.find_elements_by_class_name("bookTitle")

            for j,book in enumerate(books):
                url_new = book.get_attribute("href")
                books_urls.append(url_new)
                if url_new in map: map[url_new].append(url)
                else: map[url_new] = [url]
            logger.info(f"Collected {len(books)} books urls out of {len(books)*len(urls_big_lists)}")

        return books_urls,map

    def get_books_info(books_urls,map):
        """ retieve the info of the books
        
        Args:
            books_urls (list): list of books urls
            
            Returns:
                books_info: dict of book ids (same ID from GoodReads) and information about the book
        """
        books_info = {}

        for i,book_url in enumerate(books_urls):
            time.sleep(2)
            
            for id,row in urls.iterrows():

                book_id = i

                driver.get(book_url)
                book_name = driver.find_element_by_css_selector("#bookTitle").get_attribute("textContent").strip()
                try: 
                    try:book_abstract = driver.find_element_by_id("description").find_element_by_xpath(".//span[2]").get_attribute("textContent")
                    except: book_abstract = driver.find_element_by_id("description").find_element_by_xpath(".//span").get_attribute("textContent")
                except:
                    book_abstract = ""

                book_author = driver.find_element_by_css_selector("#bookAuthors > span:nth-child(2) > div > a > span").get_attribute("textContent")
                book_av_ratings = float(driver.find_element_by_css_selector("#bookMeta > span:nth-child(2)").get_attribute("textContent"))
                book_num_ratings = int(driver.find_element_by_xpath('//*[@id="bookMeta"]/a[2]/meta').get_attribute("content"))

                container = driver.find_element_by_class_name("rightContainer")
                # tags_list = container.find_element_by_class_name("stacked").find_elements_by_class_name("elementList")
                tags_list = container.find_elements_by_class_name("elementList")
                tags = set()
                for row in tags_list:
                    try: 
                        try: new_tags = row.find_element_by_class_name("left").text.split('>')
                        except: new_tags = row.find_element_by_xpath(".//div[1]/a").text.split('>')
                        for tag in new_tags: tags.add(tag.strip())
                    except: pass

                bookReviews = driver.find_element_by_id("bookReviews")
                bookReviews = bookReviews.find_elements_by_class_name("friendReviews")[:3]
                comments = {}
                for bookReview in bookReviews:
                    comment_author = bookReview.find_element_by_class_name("user").get_attribute("href")
                    try:
                        try:
                        comment_text = bookReview.find_element_by_xpath(".//div[1]/div[2]/span/span[2]").get_attribute("textContent").strip()
                        comments[comment_author] = comment_text
                        except:
                            comment_text = bookReview.find_element_by_xpath(".//div[1]/div[2]/span/span").get_attribute("textContent").strip()
                            comments[comment_author] = comment_text
                    except:
                        pass

                books_info[book_id] = {
                    "name":book_name,
                    "abstract":book_abstract,
                    "author":book_author,
                    "average_rating":book_av_ratings,
                    "number_of_ratings":book_num_ratings,
                    "tags": tags,
                    "comments": comments,
                    "url": book_url,
                    "list_ID": map[book_url]
                }

                # logger.info(f"Scraped {len(books_info)} books out of {len(books_urls)} books")
                sys.stdout.write(f"\rURL: {book_url}. Scraped {len(books_info)} books out of {len(books_urls)} books")
                sys.stdout.flush()

        return books_info

    def download_batches(self,books_urls,map,k=400):
        """
        Divide the dataset into k batches

        Args:
            books_urls (list): list containing the book urls
        """
        total = len(books_urls)
        batch_size = k
        num_batches = total//batch_size
        batches = [batch_size for _ in range(num_batches)]
        batches.append(total%batch_size)
        # print(batches)
        # print(len(batches))

        id = 0 # can change the id in case the scraper crashed at some instance (default id=0 starts from 0)
        for index,batch in enumerate(batches[id:]):
            i = index+id
            start = i*batch
            end = (i+1)*batch
            info_urls = books_urls[start:end]
            books_info_batch = self.get_books_info(info_urls,map)
            df_batch = pd.DataFrame(books_info_batch)
            df_batch.to_csv(f"batch_{i}.csv")
            sys.stdout.write(f"\rFinished batch {i+1}")
            sys.stdout.flush()
        info_urls = books_urls[end:]
        books_info_batch = get_books_info(info_urls,map)
        df_batch = pd.DataFrame(books_info_batch)
        df_batch.to_csv(f"batch_{i+1}.csv")
        sys.stdout.write(f"\rFinished batch {i+1}")
        sys.stdout.flush()

    def concatenate_batches(self,num_books,batch_size):
        """ concatenate all the books to one dictionary

        Args:
            num_books (int): total number of books scraped
            batch_size (int): number of books per batch

            Returns:
                books_info (pandas Dataframe): dataframe of books info
        """
        num_batches = num_batches//batch_size + int(not num_batches%batch_size==0) # ceiling of the number
        books_info = pd.DataFrame()
        for i,batch in enumerate(num_batches):
            df = pd.read_csv(f"batch_{i}.csv.csv")
            df.drop(['Unnamed: 0'], axis=1, inplace=True)
            if books_info.empty: books_info = df
            else: books_info.append(df)
        return books_info

    def download_booksUrls(self,books_urls):
        """ download the data to disk

        Args:
            books_info (dict): dict containing info of books
        """
        df = pd.DataFrame(books_urls)
        df.to_csv("urls.csv")


    def download_booksInfo(self,books_info):
        """ download the data to disk

        Args:
            books_info (pandas Dataframe): dataframe containing info of books
        """
        books_info.to_csv("goodreads_dataset.csv")

def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('headless', nargs='?', default=True, help='Run the code headless without browser')
    # args = parser.parse_args()
    # headless = args.headless

    # change to False if you want to open "chromium" browser
    open_browser = True

    # start the scraper
    scraper = GoodReadsScraper(headless=not open_browser)
    
    # get all the tags available on the webpage as a list
    tags = scraper.get_tags_list()

    # get book links as a list
    books_urls,map = scraper.get_books_urls(tags)
    # scraper.download_booksUrls(books_urls)

    # df = pd.read_csv("urls.csv",index_col=0)
    # df.rename( columns={'0':'urls'}, inplace=True)
    # books_urls = list(df.urls)

    run_batches = True

    if run_batches:
        # download the dataset as batches of size k (batch_size)
        batch_size = 400
        scraper.download_batches(books_urls,map,batch_size)
        books_info = scraper.concatenate_batches(len(books_urls),batch_size)
        scraper.download_booksInfo(books_info)
    else:
        # download all the books
        books_info = scraper.get_books_info(books_urls,map)
        books_info = pd.DataFrame(books_info)
        scraper.download_booksInfo(books_info)

if __name__=="__main__":
    main()
