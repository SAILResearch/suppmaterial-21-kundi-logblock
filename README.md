# LogBlock

### About LogBlock
This is the replication package for the paper titled "Improving State-of-the-art Compression Techniques for Log Management Tools". 

 
LogBlock is a log preprocessing tool that improves the compression of small blocks of log data.
Modern log management tools usually splits log data into small blocks to improve the performance of information query. As shown in the following table, different sizes are adopted by different log management tools 
LogBlock has better compression ratio than direct compression, or traditional log preprocessing tools which have good compression ratio on large-sized log files.


### Block sizes used by log management tools:

| Log Management Tool | Block Size | Reference |
| ------ | ------ | ------ |
| [ELK Stack](https://www.elastic.co/what-is/elk-stack) | 16KB, 60KB | https://lucene.apache.org/core/7_4_0/core/org/apache/lucene/codecs/lucene50/Lucene50StoredFieldsFormat.html |
| [Splunk](https://www.splunk.com/) | 128KB | https://static.rainfocus.com/splunk/splunkconf18/sess/1523558790516001KFjM/finalPDF/Behind-The-Magnifying-Glass-1734_1538786592130001CBKR.pdf | 
| [Sumo Logic](https://www.sumologic.com/) | 64KB | https://help.sumologic.com/05Search/Get-Started-with-Search/Search-Basics/Search-Large-Messages |
| [Syslog Ng](https://www.syslog-ng.com/) | 64KB | https://www.syslog-ng.com/technical-documents/doc/syslog-ng-open-source-edition/3.16/release-notes/summary-of-changes |
| [Nginx](https://www.nginx.com/) | 64KB | http://nginx.org/en/docs/http/ngx_http_log_module.html |
| [Rsyslog](https://www.rsyslog.com/) | 8KB | https://www.rsyslog.com/doc/master/rainerscript/global.html |
| [DataDog](https://www.datadoghq.com/) | 256KB | https://docs.datadoghq.com/api/v1/logs/ |
| [Sentry](https://sentry.io/welcome/) | 1000 characters | https://docs.sentry.io/clients/java/config/ |


### Evaluation

We include [sample logs](./logs) in this repo for evaluation purposes. 
To access the full dataset, please contact [Loghub](https://github.com/logpai/loghub).
  

The repository contains framework for evaluating different log preprocessing approaches. We take the following approaches into consideration. 
> * [LogBlock](./LogBlock) - Reduce repetitiveness through preprocessing heuritstics. 
> * [LogZip](https://github.com/logpai/logzip) - Extract reptitve template & variables through iterative clustering. Please check the full paper for more details: [Logzip: Extracting Hidden Structures via Iterative Clustering for Log Compression](https://ieeexplore.ieee.org/document/8952406). 
> * [Cowic](https://github.com/linhao1990/cowic) - Compress log entries with pretrain a compression models. Please check the full paper for more details: [Cowic: A Column-Wise Independent Compression for Log Stream Analysis](https://ieeexplore.ieee.org/document/7152468). 
> * [LogArchive](https://github.com/robertchristensen/log_archive_v0) (not taken into comparison) - Cluster log messages according to text similarity then compress. For more details, please check: [Adaptive log compression for massive log data](https://dl.acm.org/doi/10.1145/2463676.2465341).

[//]: <> (comment)
We do not include the source code of Logzip, Cowic and LogArchive due to copyright reasons. 
To evaluate these approaches from our framework, such tools should be cloned and compiled.
Start from [here](./py/main.py) to evaluate the compression performance of each approach on small logs.   
During the execution, the random truncated log blocks will be saved under 'temp' folder; the preprocess data will be recoreded under 'data' folder; and the compression performance result will be saved under 'result' folder.
All of these folders will be created at runtime.
