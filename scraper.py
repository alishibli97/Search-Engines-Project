from selenium import webdriver
from selenium.webdriver.chrome.options import Options


chrome_options = Options()
chrome_options.headless = True
driver = webdriver.Chrome(options=chrome_options)

url = "https://www.goodreads.com/list?ref=nav_brws_lists"
driver.get(url)

content = driver.find_element_by_css_selector("body > div.content > div.mainContentContainer > div.mainContent > div.mainContentFloat > div.rightContainer > div.containerWithHeader.clearFloats.bigBox > div.bigBoxBody > div")

content = content.find_elements_by_class_name("listTagsTwoColumn")

tag_url = {}
for c in content:
    for element in c.find_elements_by_class_name("actionLinkLite"):
        tag_url[element.text] = element.get_attribute('href')

base_url = "https://www.goodreads.com/list/tag/{}"

tag_urls = {}
for i,k in enumerate(tag_url.items()):
    if i>=2:
        break
    tag_urls[k[0]] = k[1]

print(tag_urls)

urls = []
for tag in tag_urls:
    driver.get(base_url.format(tag))
    lists = driver.find_element_by_class_name("listRowsFull")
    rows = lists.find_elements_by_class_name("row")
    for row in rows:
        cells = row.find_elements_by_class_name("cell")
        for cell in cells:
            url = cell.find_element_by_class_name("listImgs").find_element_by_css_selector("*").get_attribute("href")
            urls.append(url)

book_urls = []

urls = urls[:1]
for i,url in enumerate(urls):
    driver.get(url)
    books_list = driver.find_element_by_css_selector("#all_votes > table")
    books = books_list.find_elements_by_class_name("bookTitle")
    
    for j,book in enumerate(books):
        # print(str(j)+" "+book.get_attribute("href"))
        book_urls.append(book.get_attribute("href"))
    
    # print(f"Finished {i}: {len(books)}")

books_info = {}

for i,book_url in enumerate(book_urls):

    book_id = int(book_url.rsplit('/', 1)[-1].split('-')[0].split('.')[0])

    if book_id in books_info: break
    
    driver.get(book_url)
    book_name = driver.find_element_by_css_selector("#bookTitle").get_attribute("textContent")
    book_abstract = driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[2]/div[4]/div[1]/div[2]/div[3]/div/span[2]").get_attribute("textContent")
    book_author = driver.find_element_by_css_selector("#bookAuthors > span:nth-child(2) > div > a > span").get_attribute("textContent")
    book_rating = float(driver.find_element_by_css_selector("#bookMeta > span:nth-child(2)").get_attribute("textContent"))

    book_tags = driver.find_elements_by_class_name("actionLinkLite bookPageGenreLink")

    for book_tag in book_tags:
        print(book_tag.text)

    books_info[book_id] = {
        "name":book_name,
        "abstract":book_abstract,
        "author":book_author,
        "rating":book_rating,
        # "tag": set(tags)
    }