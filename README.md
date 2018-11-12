# ElasticSearchDemo
elasticSearch的一些demo，以及elasticSearch的一些学习资源

### NoNodeAvailableException报错，主要是外网IP没有配置
在elasticSearch.yml上配置transport.host和transport.tcp.port即可<br/>
>transport.host: localhost<br/>
transport.tcp.port: 9300

https://stackoverflow.com/questions/47881049/unable-to-connect-to-elasticsearch-6-1-0none-of-the-configured-nodes-are-availa
