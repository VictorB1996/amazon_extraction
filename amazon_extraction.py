import requests, pandas as pd
from bs4 import BeautifulSoup
import time, random

# Read the content of the file which contains 100 random user agents
with open(r"user_agents.txt", 'r') as file:
    content = file.read()
    user_agents = content.split('\n')

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    'accept-language': 'en-US,en;q=0.9',
    "connection": "keep-alive",
    "content-type": "text/plain;charset=UTF-8",
    "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
    "sec-ch-ua": "?0",
    "sec-ch-ua-platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": random.choice(user_agents),

    'authority': 'www.amazon.com',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'dnt': '1',
    'upgrade-insecure-requests': '1',
    'sec-fetch-dest': 'document'
}

def scrape_product_reviews(url, product_code, cookies = None, page = 1, results = []):
    print(url)
    try:
        response = requests.get(url = url, headers = headers, cookies = None)   
        document = BeautifulSoup(response.content, "html.parser")

        reviews_holder = document.find("div", {"id": "cm_cr-review_list"})
        reviews = reviews_holder.find_all("div", attrs = {"data-hook": "review"})
    except Exception:
        response = requests.get(url = url, headers = headers, cookies = cookies)   
        document = BeautifulSoup(response.content, "html.parser")

        reviews_holder = document.find("div", {"id": "cm_cr-review_list"})
        reviews = reviews_holder.find_all("div", attrs = {"data-hook": "review"})
    # print(document)

    for review in reviews:
        review_title = review.find(class_ = "review-title").text
        review_body = review.find("span", attrs = {"data-hook": "review-body"}).text

        results.append(
            {
                "product_code": product_code,
                "review_title": review_title.replace("\n", ""),
                # "review_page": page,
                "review_body": review_body.replace("\n", "").replace(" The media could not be loaded.", "").lstrip().rstrip()
            }
        )
    # print(results)
    # print(document)
    try:
        pagination = document.find("ul", class_ = "a-pagination").find_all("li")
        next_page = pagination[1]
    except Exception:
        return results

    if "a-disabled" in next_page["class"]:
        return results
    else:
        if cookies == None:
            session = requests.Session()
            cookies = response.cookies 
        page = page + 1
        if page > 50:
            return results
        next_page_url = "https://www.amazon.com/product-reviews/{}".format(product_code) \
            + "/ref=cm_cr_getr_d_paging_btm_next_{}?pageNumber={}".format(page, page)
        # print(next_page_url)
        # time.sleep(3)
        return scrape_product_reviews(next_page_url, product_code, cookies = cookies, page = page, results = results)
        

def get_products_for_all_pages(url, product_codes_list = [], cookies = None):
    print(url)
    response = requests.get(url, headers = headers, cookies = cookies)
    document = BeautifulSoup(response.content, "html.parser")

    products = document.find_all("div", attrs = {"data-component-type": "s-search-result"})
    product_codes = [p["data-asin"] for p in products]
    product_codes_list.extend(product_codes)

    next_page = document.find("a", class_ = ["s-pagination-next"])
    if next_page:
        if cookies == None:
            session = requests.Session()
            cookies = response.cookies 

        next_page_url = "https://www.amazon.com/" + next_page["href"]
        return get_products_for_all_pages(next_page_url, product_codes_list, cookies = cookies)
    else:
        print("Found {} products. Getting Reviews...".format(len(set(product_codes_list))))
        return set(product_codes_list)
    
def wrapper(starting_url):
    category_results = []
    product_codes = get_products_for_all_pages(starting_url)
    base_url = "https://www.amazon.com/product-reviews/"
    for code in product_codes:
        results = []
        url = base_url + code
        # print(url)
        results = scrape_product_reviews(url, code)
        category_results.extend(results)
    return category_results

import concurrent.futures

def wrapper_multithread(starting_url):
    category_results = []
    product_codes = get_products_for_all_pages(starting_url)
    base_url = "https://www.amazon.com/product-reviews/"
    with concurrent.futures.ThreadPoolExecutor(max_workers= 10) as executor:
        futures = {executor.submit(scrape_product_reviews, base_url + code, code): code for code in product_codes}
        for future in concurrent.futures.as_completed(futures):
            results = future.result()
            category_results.extend(results)
    return category_results

starting_url = "https://www.amazon.com/s?i=kitchen-intl-ship&bbn=16225011011&rh=n%3A1063306%2Cn%3A1063318%2Cn%3A680098011%2Cn%3A3733631&dc&ds=v1%3AR%2Fn%2FXM%2BESmpubVcbu3s2BDqxy68BBkfvm%2FG2UuFzQDI&qid=1682080333&rnid=680098011&ref=sr_nr_n_1"

reviews = wrapper_multithread(starting_url)

df = pd.DataFrame(reviews).drop_duplicates()
df.to_excel("reviews.xlsx")