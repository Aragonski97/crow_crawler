# loop_crawler
A general purpose HTML webcrawler that utilizes python asynchronous and multiprocessing capabilities to quickly download and process the data.
The crawler expects mandatory foreign_id column which represents id of the final product and it has to be int. This was for my uses only, can easily be tweaked in ConnectionObject.upload_file.
It's fullest potential can be seen when working with paginated architectures such as Ebay's, although I had no need of crawling Ebay in particular.

Have in mind that I am working on this project for a couple of months now, so my skills have progressed, which is why you might encounter code which is not uniformly written. Initially it was supposed to be much smaller, but as my skills grew so did my curiosity.

Loop Crawler currently supports MySQL database connection only, but since the underlying engine / ORM is SQLAlchemy, this can easily be tweaked to support other SQL databases.
There is also support for AWS S3 architecture, although this was only used in testing.
An end-user can decide to print a csv that consists of the downloaded data. In which case it is being saved to Hosts folder within the project. Please do not move this folder as secondary and following recrawls search for config within it.

Loop Crawler is inteded for multiprocess use so it is recommended to run it on 4-core processors for full support. 

As this webcrawler is intended for a single pc, it is self-evident that one has to use proxies in order to avoid getting blocked by websites. However, I realized that this will slow down the crawl due to proxy latency and will incur additional support costs such as proxy provider's etc. I decided to take another route and start buying Raspberry Pis and giving them to my friends so as to use their IPs and have the load distributed to multiple nodes which saves both time and memory. Distributed computing on Raspberry Pies is both a great excercise and investment as they can serve as worker nodes later down the stream, once I start working on my Machine Learning projects, this will save money in the long run as compared to paying for proxy services.

This Loop Crawler Distributed project is still in development and will take some time to finish as I'm studying and working full time in parallel to working on this project.
That being said, I will continue to provide commits to the Loop Crawler OG whenever I can. I have already remade most of the modules in V2 and will commit them soon.
I have a transformer class that I plan to implement based on pandas/numpy to transform the crawled data such as setting the right types of data, etc. However, most likely the end users will have to define pandas instructions by themselves.
