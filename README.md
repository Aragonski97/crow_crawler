# loop_crawler
A general purpose HTML webcrawler that utilizes python asynchronous and multiprocessing capabilities to quickly download and process the data.
It's fullest potential can be seen when working with paginated architectures such as Ebay's, although I had no need of crawling Ebay in particular.

Loop Crawler currently supports MySQL database connection only, but since the underlying engine is SQLAlchemy, this can easily be tweaked to support other SQL databases.
There is also support for AWS S3 architecture, although this was only used in testing.
An end-user can decide to print a csv that consists of the downloaded data. In which case it is being saved to Hosts folder within the project. Please do not move this folder as secondary and following recrawls search for config within it.

Loop Crawler is inteded for multiprocess use so it is recommended to run it on 4-core processors for full support. 

As this webcrawler is intended for a single pc, it is self-evident that one has to use proxies in order to get blocked by websites. However, I realized that this will slow down the crawl due to proxy latency and will incur additional support costs such as proxy provider's etc. I decided to take another route and start buying Raspberry Pis and giving them to my friends so as to use their IPs and have the load distributed to multiple nodes which saves both time and memory. Distributed computing on Raspberry Pies is both a great excercise and investment as they can serve as worker nodes later down the stream, once I start working on my Machine Learning projects, this will save money in the long run as compared to paying for proxy services.

This Loop Crawler Distributed project is still in development and will take some time to finish as I'm studying and working in parallel to working on this project.
That being said, I will continue to provide commits to the Loop Crawler OG whenever I can.
