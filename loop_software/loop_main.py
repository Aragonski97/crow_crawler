from loop_software.loop_crawler import HTMLCrawler, URLCrawler
from loop_software.loop_webhost import open_or_create_host
from loop_software.loop_selector import RegexSelector, XpathSelector, StaticSelector, JsonSelector, WebSelector
from loop_software.loop_connections import CSVConnection, SQLConnection, AWSConnection


def main():
    # example of the structure, not to be run without adequate configuration for the website.

    host = open_or_create_host(host_id=1,  # random number
                               name="myurl",  # for instance
                               top_level_domain="com")  # for instance, could be org
    # have the schema / database created previously
    connection = SQLConnection(
        connection_config={"user": "user_name", "password": "password", "host": "IP or name", "database": "w/e",
                           "use_pure": True, "autocommit": True})
    # you can create CSV first if you'd like
    # connection = CSVConnection(connection_config={"path": host.path})
    host.request_settings = {"connection_limit": 10,  # number of async connections to the server,
                                                      # test server to find the best
                             "trust_env": True,
                             "headers": {"key": "value"},
                             "batch_length": 200,  # specify the batch length, 200 by default
                             "save_none": False}
    url_cache = URLCrawler(webhost=host, urls=["https://myurl.com"])  # for instance
    """
    Modify your url_cache as desired.
    Example of paginated website: 
    First it gets pagination count if possible
    Then iterates with the given page through pagination count with desired payload so as to set urls correctly
    Then crawls for the "primary urls" (bread and butter).
    Finally prepends seed to the extracted primary urls. Not usually necessary, but comes in handy later on.
    """
    url_cache \
        .get_pages_number(pages_selector=RegexSelector(name="pages_number", directive=r"records\:\s?(\d+),")) \
        .get_urls_formatted_with_pages(payload={"page": r"{0}",
                                                "sort": "basic",
                                                "city_distance": "0",
                                                "showOldNew": "all",
                                                "without_price": "1"},
                                       # optional
                                       pages_callable=lambda x: int(x / 1000))\
        .crawl(levels=[[XpathSelector(name="content_containing_urls",
                                      directive=r"//div[@class='textContent']/h2/a[@onclick]/@href", method="any")]]) \
        .prepend_seed(seed="https://www.website.com")
    content_selectors = [XpathSelector(name="price",
                                       directive=r"//span[@class='priceClassified regularPriceColor']/text()"),
                         RegexSelector(name="city", directive=r'"object_city":"([^"]+)"'),
                         JsonSelector(name="url", directive=r'["url"]')]
    host.content_selectors = content_selectors
    main_crawl = HTMLCrawler(webhost=host, urls=url_cache.urls)
    main_crawl.crawl(connection=connection)
