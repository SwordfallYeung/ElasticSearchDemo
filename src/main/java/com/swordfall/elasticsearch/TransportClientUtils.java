package com.swordfall.elasticsearch;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.*;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;

/**
 * @Author: Yang JianQiu
 * @Date: 2018/11/12 16:40
 *
 * transportClient将会在7.0版本上过时，并在8.0版本上移除掉，建议使用Java High Level REST Client
 */
public class TransportClientUtils {

    private TransportClient client = null;

    public TransportClientUtils() {
        if (client == null){
            synchronized (TransportClientUtils.class){
                if (client == null){
                    client = getClient();
                }
            }
        }
    }

    public TransportClient getClient(){
        TransportClient client = null;
        try {
            Settings settings = Settings.builder()
                    .put("client.transport.sniff", true)
                    .put("cluster.name", "bigdata").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(new InetSocketAddress("192.168.187.201", 9300)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    /*------------------------------------------- 单操作 -----------------------------------------------*/

    /**
     * 增，插入记录
     * @throws Exception
     */
    public void index(String json) throws Exception{
        IndexResponse response = client.prepareIndex("twitter", "_doc")
                .setSource(json, XContentType.JSON)
                .get();

        //Index name
        String _index = response.getIndex();
        //Type name
        String _type = response.getType();
        //Document ID (generated or not)
        String _id = response.getId();
        //Version
        long _version = response.getVersion();
        //status has stored current instance statement
        RestStatus status = response.status();

        System.out.println("index: " + _index);
        System.out.println("type: " + _type);
        System.out.println("id: " + _id);
        System.out.println("version: " + _version);
        System.out.println("status: " + status);
    }

    /**
     * 根据Id查询
     * @param id
     * @throws Exception
     */
    public void get(String id) throws Exception{
        GetResponse response = client.prepareGet("twitter", "_doc", id).get();
        Map<String, Object> map = response.getSource();
        for (String key: map.keySet()){
            System.out.println(key + ": " + map.get(key).toString());
        }
    }

    /**
     * 根据Id删除
     * @param id
     * @throws Exception
     */
    public void delete(String id) throws Exception{
        DeleteResponse response = client.prepareDelete("twitter", "_doc", id).get();
    }

    /**
     * 根据查询条件删除
     * @throws Exception
     */
    public void deleteByQueryAPI() throws Exception{
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "kimchy"))
                .source("twitter")
                .get();

        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    /**
     * 根据查询条件删除并返回结果数据 不起作用
     * @throws Exception
     */
    public void deleteByQueryAPIWithListener() throws Exception{
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "kimchy"))
                .source("twitter")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        if (deleted == 1){
                            System.out.println("删除成功");
                        }
                        System.out.println("删除中...");
                    }

                    @Override
                    public void onFailure(Exception e) {
                          //Handle the exception
                        System.out.println("删除失败");
                    }
                });
        System.out.println("hahahahahah");
    }

    /**
     * 更新update
     * @throws Exception
     */
    public void update() throws Exception{
        //Method One
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter");
        updateRequest.type("_doc");
        updateRequest.id("NpEWCGcBi36MQkKOSdf3");
        updateRequest.doc(jsonBuilder()
                 .startObject()
                 .field("user", "tom")
                .endObject()
        );

        client.update(updateRequest).get();

        //Method Two
        UpdateRequest updateRequest1 = new UpdateRequest("twitter", "_doc", "NpEWCGcBi36MQkKOSdf3")
                .doc(jsonBuilder()
                .startObject()
                .field("user", "tom")
                .endObject()
        );

        client.update(updateRequest1).get();

        //Method Three
        UpdateRequest updateRequest2 = new UpdateRequest("twitter", "_doc", "NpEWCGcBi36MQkKOSdf3")
                .script(new Script("ctx._source.user = \"tom1\""));
        client.update(updateRequest2).get();
    }

    /**
     * 更新prepareUpdate
     * @throws Exception
     */
    public void prepareUpdate() throws Exception{
        client.prepareUpdate("twitter", "_doc", "NpEWCGcBi36MQkKOSdf3")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("user", "tom")
                        .endObject())
                .get();
    }

    /**
     * 如果不存在则创建indexRequest，存在则更新updateRequest
     * @throws Exception
     */
    public void upsert() throws Exception{
        IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                .startObject()
                .field("name", "Joe Smith")
                .field("gender", "male")
                .endObject());

        UpdateRequest updateRequest = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
        client.update(updateRequest);
    }

    /*------------------------------------------- 多操作 -----------------------------------------------*/

    /**
     * 指定单个Id获取，指定多个Id获取，从另外一个库表获取数据
     * @throws Exception
     */
    public void multiGet() throws Exception{
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("twitter", "_doc", "1")
                .add("twitter", "_doc", "2", "3", "4")
                .add("another", "_doc", "foo")
                .get();

        for (MultiGetItemResponse itemResponse: multiGetItemResponses){
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()){
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    /**
     * 批量处理
     * @throws Exception
     */
    public void bulkAPI() throws Exception{
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        //either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("twitter", "_doc", "1")
             .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                     .endObject()
             )
        );

        bulkRequest.add(client.prepareIndex("twitter", "_doc", "2")
                .setSource(jsonBuilder()
                           .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                )
        );

        BulkResponse bulkResponses = bulkRequest.get();

        if (bulkResponses.hasFailures()){
            // process failures by iterating through each bulk response item
            System.out.println("报错");
        }
    }

    /**
     * 创建自定义的批处理器
     * @throws Exception
     */
    public void createBulkProcessor() throws Exception{
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long l, BulkRequest bulkRequest) {
                        System.out.println("beforeBulk...");
                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        System.out.println("成功，afterBulk...");
                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                        System.out.println("失败，afterBulk...");
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)
                ).build();

        //添加批量处理操作
        bulkProcessor.add(new IndexRequest("twitter", "_doc", "1")
                      .source(jsonBuilder()
                              .startObject()
                              .field("user", "kimchy")
                              .field("postDate", new Date())
                              .field("message", "trying out Elasticsearch")
                              .endObject()));

        bulkProcessor.add(new DeleteRequest("twitter", "_doc", "2"));

        //flush any remaining requests
        bulkProcessor.flush();

        //Or close the bulkProcess if you don't need it anymore
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        //or
        //bulkProcessor.close();

        //Refresh your indices
        client.admin().indices().prepareRefresh().get();

        //Now you can start searching
        client.prepareSearch().get();
    }

    /**
     * 根据查询条件更新
     * @throws Exception
     */
    public void updateByQueryAPI() throws Exception{
        //Method One
        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery.source("source_index").abortOnVersionConflict(false);
        BulkByScrollResponse response = updateByQuery.get();

        //Method Two
        UpdateByQueryRequestBuilder updateByQuery1 = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery1.source("source_index")
                .filter(termQuery("level", "awesome"))
                .size(1000)
                .script(new Script(ScriptType.INLINE, "ctx._source.awesome = 'absolutely'", "painless", Collections.emptyMap()));
        BulkByScrollResponse response1 = updateByQuery1.get();

        //Method Three
        UpdateByQueryRequestBuilder updateByQuery2 = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery2.source("source_index").size(100)
                .source().addSort("cat", SortOrder.DESC);
        BulkByScrollResponse response2 = updateByQuery2.get();

        //Method Four
        UpdateByQueryRequestBuilder updateByQuery3 = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery3.source("source_index")
                .script(new Script(
                        ScriptType.INLINE,
                        "if (ctx._source.awesome == 'absolutely') {" +
                        " ctx.op = 'noop'" +
                        "} else if (ctx._source.awesome == 'lame') {" +
                        " ctx.op = 'delete'" +
                        "} else {" +
                        "ctx._source.awesome = 'absolutely'" +
                        "}",
                        "painless",
                        Collections.emptyMap()
                ));
        BulkByScrollResponse response3 = updateByQuery3.get();

        //Method Five
        UpdateByQueryRequestBuilder updateByQuery4 = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery4.source("foo", "bar").source().setTypes("a", "b");
        BulkByScrollResponse response4 = updateByQuery4.get();

        //Method Six
        UpdateByQueryRequestBuilder updateByQuery5 = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery5.source().setRouting("cat");
        BulkByScrollResponse response5 = updateByQuery5.get();

        //Method Seven
        UpdateByQueryRequestBuilder updateByQuery6 = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery6.setPipeline("hurray");
        BulkByScrollResponse response6 = updateByQuery6.get();

        //Method Eight works with the task api
        ListTasksResponse tasksList = client.admin().cluster().prepareListTasks()
                .setActions(UpdateByQueryAction.NAME).setDetailed(true).get();
        for (TaskInfo info: tasksList.getTasks()){
            TaskId taskId = info.getTaskId();
            BulkByScrollTask.Status status = (BulkByScrollTask.Status) info.getStatus();
            //do stuff

            GetTaskResponse get = client.admin().cluster().prepareGetTask(taskId).get();

            //works with the cancel task api
            // Cancel all update-by-query requests
            client.admin().cluster().prepareCancelTasks().setActions(UpdateByQueryAction.NAME).get().getTasks();
            // Cancel a specific update-by-query request
            client.admin().cluster().prepareCancelTasks().setTaskId(taskId).get().getTasks();

            //Rethrottling
            RethrottleAction.INSTANCE.newRequestBuilder(client)
                    .setTaskId(taskId)
                    .setRequestsPerSecond(2.0f)
                    .get();
        }
    }

    public void Reindex() throws Exception{
        BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client)
                .destination("target_index")
                .filter(QueryBuilders.matchQuery("category", "xzy"))
                .get();
    }

    /*------------------------------------------- search Api start -----------------------------------------------*/

    public void searchAPI() throws Exception{
        SearchResponse response = client.prepareSearch("index1", "index2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(termQuery("multi", "test"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))
                .setFrom(0).setSize(60).setExplain(true)
                .get();

        SearchResponse response1 = client.prepareSearch().get();
    }

    /**
     * 卷轴
     * @throws Exception
     */
    public void scrolls() throws Exception{
        QueryBuilder qb = termQuery("multi", "test");

        SearchResponse scrollResp = client.prepareSearch("test")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100)
                .get();

        //Scroll until no hits are returned
        do {
            for (SearchHit hit: scrollResp.getHits().getHits()){
                //Handle the hit...
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }while (scrollResp.getHits().getHits().length != 0);
        //Zero hits mark the end of the scroll and the while loop.
    }

    /**
     * 多查询
     * @throws Exception
     */
    public void multiSearch() throws Exception{
        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();

        //You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;
        for (MultiSearchResponse.Item item: sr.getResponses()){
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
        }
    }

    /*------------------------------------------- search Api end -----------------------------------------------*/

    /*------------------------------------------- Aggregations start -----------------------------------------------*/

    public void aggregations() throws Exception{
        SearchResponse sr = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("agg1").field("field")
                )
                .addAggregation(
                        AggregationBuilders.dateHistogram("agg2")
                        .field("birth")
                        .dateHistogramInterval(DateHistogramInterval.YEAR)
                )
                .get();

        //Get your facet results
        Terms agg1 = sr.getAggregations().get("agg1");
        Histogram agg2 = sr.getAggregations().get("agg2");
    }

    public void terminateAfter() throws Exception{
        SearchResponse sr = client.prepareSearch("")
                .setTerminateAfter(1000)
                .get();

        if (sr.isTerminatedEarly()){
            // We finished early
        }
    }

    /**
     * 查询模版
     * @throws Exception
     */
    public void searchTemplate() throws Exception{
        Map<String, Object> template_params = new HashMap<>();
        template_params.put("param_gender", "male");

        //create your search template request
        SearchResponse sr = new SearchTemplateRequestBuilder(client)
                .setScript("template_gender")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();

        SearchResponse sr1 = new SearchTemplateRequestBuilder(client)
                .setScript("template_gender")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();

        SearchResponse sr2 = new SearchTemplateRequestBuilder(client)
                .setScript("{\n" +
                        "       \"query\" : {\n " +
                        "             \"match\" : {\n " +
                        "                   \"gender\" : \"{{param_gender}}\"\n" +
                        "               }\n" +
                        "         }\n" +
                        "}")
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();
    }

    /**
     * 构造聚合
     * @throws Exception
     */
    public void structuringAggregations() throws Exception{
        SearchResponse sr = client.prepareSearch()
                .addAggregation(
                        AggregationBuilders.terms("by_country").field("country")
                        .subAggregation(AggregationBuilders.dateHistogram("by_year")
                                .field("dateOfBirth")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_children")
                                .field("children"))
                        )
                )
                .execute()
                .actionGet();
    }

    public void metricsAggregations() throws Exception{
        //Min Aggregation
        MinAggregationBuilder aggregation = AggregationBuilders
                .min("agg")
                .field("height");
        SearchResponse sr = client.prepareSearch().addAggregation(aggregation).execute().actionGet();
        Min agg = sr.getAggregations().get("agg");
        double value = agg.getValue();

        //Max Aggregation
        MaxAggregationBuilder aggregation1 = AggregationBuilders
                .max("agg")
                .field("height");
        SearchResponse sr1 = client.prepareSearch().addAggregation(aggregation1).execute().actionGet();
        Max agg1 = sr1.getAggregations().get("agg");
        double value1 = agg1.getValue();

        //Sum Aggregation
        SumAggregationBuilder aggregation2 = AggregationBuilders
                .sum("agg")
                .field("height");
        SearchResponse sr2 = client.prepareSearch().addAggregation(aggregation2).execute().actionGet();
        Sum agg2 = sr2.getAggregations().get("agg");
        double value2 = agg2.getValue();

        //Avg Aggregation
        AvgAggregationBuilder aggregation3 = AggregationBuilders
                .avg("agg")
                .field("height");
        SearchResponse sr3 = client.prepareSearch().addAggregation(aggregation3).execute().actionGet();
        Avg agg3 = sr3.getAggregations().get("agg");
        double value3 = agg3.getValue();

        //Stats Aggregation
        StatsAggregationBuilder aggregation4 = AggregationBuilders
                .stats("agg")
                .field("height");
        SearchResponse sr4 = client.prepareSearch().addAggregation(aggregation4).execute().actionGet();
        Stats agg4 = sr4.getAggregations().get("agg");
        double min = agg4.getMin();
        double max = agg4.getMax();
        double avg = agg4.getAvg();
        double sum = agg4.getSum();
        long count = agg4.getCount();

        //Extended Stats Aggregation
        ExtendedStatsAggregationBuilder aggregation5 = AggregationBuilders
                .extendedStats("agg")
                .field("height");
        SearchResponse sr5 = client.prepareSearch().addAggregation(aggregation5).execute().actionGet();
        ExtendedStats agg5 = sr5.getAggregations().get("agg");
        double min1 = agg5.getMin();
        double max1 = agg5.getMax();
        double avg1 = agg5.getAvg();
        double sum1 = agg5.getSum();
        long count1 = agg5.getCount();
        double stdDeviation = agg5.getStdDeviation();
        double sumOfSquares = agg5.getSumOfSquares();
        double variance = agg5.getVariance();

        //Value Count Aggregation
        ValueCountAggregationBuilder aggregation6 = AggregationBuilders
                .count("agg")
                .field("height");
        SearchResponse sr6 = client.prepareSearch().addAggregation(aggregation6).execute().actionGet();
        ValueCount agg6 = sr6.getAggregations().get("agg");
        double value4 = agg6.getValue();

        //Prepare Aggregation
        PercentilesAggregationBuilder aggregation7 = AggregationBuilders
                .percentiles("agg")
                .field("height");
              //.percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);
        SearchResponse sr7 = client.prepareSearch().addAggregation(aggregation7).execute().actionGet();
        Percentiles agg7 = sr7.getAggregations().get("agg");
        for (Percentile entry: agg7){
            // Percent
            double percent = entry.getPercent();
            // Value
            double value7 = entry.getValue();

            System.out.println("percent [{ " + percent + " }], value [{ " + value7 + " }]");
        }

        //Cardinality Aggregation
        CardinalityAggregationBuilder aggregation8 = AggregationBuilders
                .cardinality("agg")
                .field("tags");
        //.percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);
        SearchResponse sr8 = client.prepareSearch().addAggregation(aggregation8).execute().actionGet();
        Cardinality agg8 = sr8.getAggregations().get("agg");
        long value8 = agg8.getValue();

       //Geo Bounds Aggregation
        GeoBoundsAggregationBuilder aggregation9 = AggregationBuilders
                .geoBounds("agg")
                .field("address.location");
        //.percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);
        SearchResponse sr9 = client.prepareSearch().addAggregation(aggregation9).execute().actionGet();
        GeoBounds agg9 = sr9.getAggregations().get("agg");
        GeoPoint bottomRight = agg9.bottomRight();
        GeoPoint topLeft = agg9.topLeft();
        System.out.println("bottomRight { " + bottomRight + " }, topLeft { " + topLeft + " }");

        //Top Hits Aggregation
        AggregationBuilder aggregation10 = AggregationBuilders
                .terms("agg")
                .field("gender")
                .subAggregation(
                        AggregationBuilders.topHits("top")
                        /*AggregationBuilders.topHits("top")
                                .explain(true)
                                .size(1)
                                .from(10)*/
                );
        //.percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);
        SearchResponse sr10 = client.prepareSearch().addAggregation(aggregation10).execute().actionGet();
        Terms agg10 = sr10.getAggregations().get("agg");
        for (Terms.Bucket entry: agg10.getBuckets()){
            String key = entry.getKey().toString();
            long docCount = entry.getDocCount();
            System.out.println("key [{ " + key + " }], doc_count [{ " + docCount + " }]");

            TopHits topHits = entry.getAggregations().get("top");
            for (SearchHit hit: topHits.getHits().getHits()){
                System.out.println(" -> id [{ " + hit.getId() + " }], _source [{ " + hit.getSourceAsString() + " }]");
            }
        }

        //Scripted Metric Aggregation
        ScriptedMetricAggregationBuilder aggregation11 = AggregationBuilders
                .scriptedMetric("agg")
                .initScript(new Script("state.heights = []"))
                .mapScript(new Script("state.heights.add(doc.gender.value == 'male' ? doc.height.value : -1.0 * doc.height.value)"))
                .combineScript(new Script("double heights_sum = 0.0; for (t in state.heights) { heights_sum += t } return heights_sum"));
                //.reduceScript(new Script("double heights_sum = 0.0; for (a in states) { heights_sum += a } return heights_sum"));
        SearchResponse sr11 = client.prepareSearch().addAggregation(aggregation11).execute().actionGet();
        ScriptedMetric agg11 = sr11.getAggregations().get("agg");
        Object scriptedResult = agg11.aggregation();
        System.out.println("scriptedResult [{ " + scriptedResult + " }]");
    }

    /**
     * 批量聚合
     * @throws Exception
     */
    public void bucketAggregations() throws Exception{
        //Global Aggregation
        GlobalAggregationBuilder aggregation = AggregationBuilders.global("agg")
                .subAggregation(
                        AggregationBuilders.terms("genders").field("gender")
                );
        SearchResponse sr = client.prepareSearch().addAggregation(aggregation).execute().actionGet();
        Global agg = sr.getAggregations().get("agg");
        agg.getDocCount();

        //Filter Aggregation
        FilterAggregationBuilder aggregation1 = AggregationBuilders.filter("agg",
                QueryBuilders.termQuery("gender", "male"));
        SearchResponse sr1 = client.prepareSearch().addAggregation(aggregation1).execute().actionGet();
        Filter agg1 = sr1.getAggregations().get("agg");
        agg1.getDocCount();

        //Filters Aggregation
        FiltersAggregationBuilder aggregation2 = AggregationBuilders.filters("agg",
                new FiltersAggregator.KeyedFilter("men", QueryBuilders.termQuery("gender", "male")),
                new FiltersAggregator.KeyedFilter("women", QueryBuilders.termQuery("gender", "female")));
        SearchResponse sr2 = client.prepareSearch().addAggregation(aggregation2).execute().actionGet();
        Filters agg2 = sr2.getAggregations().get("agg");
        for (Filters.Bucket entry: agg2.getBuckets()){
            String key = entry.getKeyAsString();
            long docCount = entry.getDocCount();
            System.out.println("key [{ " + key + " }], doc_count [{ " + docCount + " }]");
        }

        //Missing Aggregation
        MissingAggregationBuilder aggregation3 = AggregationBuilders.missing("agg").field("gender");
        SearchResponse sr3 = client.prepareSearch().addAggregation(aggregation3).execute().actionGet();
        Missing agg3 = sr3.getAggregations().get("agg");
        agg3.getDocCount();

        //Nested Aggregation
        NestedAggregationBuilder aggregation4 = AggregationBuilders.nested("agg", "resellers");
        SearchResponse sr4 = client.prepareSearch().addAggregation(aggregation4).execute().actionGet();
        Nested agg4 = sr4.getAggregations().get("agg");
        agg4.getDocCount();

        //Reverse Nested Aggregation
        NestedAggregationBuilder aggregation5 = AggregationBuilders
                .nested("agg", "resellers")
                .subAggregation(
                        AggregationBuilders
                                .terms("name")
                                .field("resellers.name")
                                .subAggregation(
                                        AggregationBuilders.reverseNested("reseller_to_product")
                                )
                );
        SearchResponse sr5 = client.prepareSearch().addAggregation(aggregation5).execute().actionGet();
        Nested agg5 = sr5.getAggregations().get("agg");
        Terms name = agg5.getAggregations().get("name");
        for (Terms.Bucket bucket: name.getBuckets()){
            ReverseNested resellerToProduct = bucket.getAggregations().get("reseller_to_product");
            resellerToProduct.getDocCount();
        }

        //Children Aggregation
        /*AggregationBuilder aggregation6 = AggregationBuilders.children("agg", "reseller");
        SearchResponse sr6 = client.prepareSearch().addAggregation(aggregation6).execute().actionGet();
        Children agg6 = sr6.getAggregations().get("agg");
        agg6.getDocCount();*/

        //Terms Aggregation
        AggregationBuilder aggregation7 = AggregationBuilders
                .terms("genders")
                .field("gender");
        SearchResponse sr7 = client.prepareSearch().addAggregation(aggregation7).execute().actionGet();
        Terms genders = sr7.getAggregations().get("genders");
        for (Terms.Bucket entry: genders.getBuckets()){
            entry.getKey();
            entry.getDocCount();
        }

        //Order
        AggregationBuilders.terms("genders")
                .field("gender")
                .order(BucketOrder.count(true));
        AggregationBuilders.terms("genders")
                .field("gender")
                .order(BucketOrder.key(true));
        AggregationBuilders.terms("genders")
                .field("gender")
                .order(BucketOrder.aggregation("avg_height", false))
                .subAggregation(
                        AggregationBuilders.avg("avg_height").field("height")
                );
        AggregationBuilders.terms("genders")
                .field("gender")
                .order(BucketOrder.compound(
                        BucketOrder.aggregation("avg_height", false),
                        BucketOrder.count(true)
                ))
                .subAggregation(
                        AggregationBuilders.avg("avg_height").field("height")
                );

        //Significant Terms Aggregation
        AggregationBuilder aggregation9 = AggregationBuilders
                .significantTerms("significant_countries")
                .field("address.country");
        SearchResponse sr9 = client.prepareSearch()
                .setQuery(QueryBuilders.termQuery("gender", "male"))
                .addAggregation(aggregation9)
                .get();
        SignificantTerms agg9 = sr9.getAggregations().get("significant_countries");
        for (SignificantTerms.Bucket entry: agg9.getBuckets()){
            entry.getKey();
            entry.getDocCount();
        }

        //Range Aggregation
        AggregationBuilder aggregation10 = AggregationBuilders
                .range("agg")
                .field("height")
                .addUnboundedTo(1.0f)
                .addRange(1.0f, 1.5f)
                .addUnboundedFrom(1.5f);
        SearchResponse sr10 = client.prepareSearch().addAggregation(aggregation10).execute().actionGet();
        Range agg10 = sr10.getAggregations().get("agg");
        for (Range.Bucket entry: agg10.getBuckets()){
            String key = entry.getKeyAsString();
            Number from = (Number) entry.getFrom();
            Number to = (Number) entry.getTo();
            long docCount = entry.getDocCount();
            System.out.println("key [{ " + key + " }], from [{ " + from + " }], to [{ " + to + " }], doc_count [{ " + docCount + " }]");
        }

        //Date Range Aggregation
        AggregationBuilder aggregation11 = AggregationBuilders
                .dateRange("agg")
                .field("dateOfBirth")
                .format("yyyy")
                .addUnboundedTo("1950")
                .addRange("1950", "1960")
                .addUnboundedFrom("1960");
        SearchResponse sr11 = client.prepareSearch().addAggregation(aggregation11).execute().actionGet();
        Range agg11 = sr11.getAggregations().get("agg");
        for (Range.Bucket entry: agg11.getBuckets()){
            String key = entry.getKeyAsString();
            DateTime fromAsDate = (DateTime) entry.getFrom();
            DateTime toAsDate = (DateTime) entry.getTo();
            long docCount = entry.getDocCount();
            System.out.println("key [{ " + key + " }], from [{ " + fromAsDate + " }], to [{ " + toAsDate + " }], doc_count [{ " + docCount + " }]");
        }

        //In Range Aggregation
        AggregationBuilder aggregation12 = AggregationBuilders
                .ipRange("agg")
                .field("ip")
                .addUnboundedTo("192.168.1.0")
                .addRange("192.168.1.0", "192.168.2.0")
                .addUnboundedFrom("192.168.2.0");
        /*AggregatorBuilder<?> aggregation =
                AggregationBuilders
                        .ipRange("agg")
                        .field("ip")
                        .addMaskRange("192.168.0.0/32")
                        .addMaskRange("192.168.0.0/24")
                        .addMaskRange("192.168.0.0/16");*/
        SearchResponse sr12 = client.prepareSearch().addAggregation(aggregation12).execute().actionGet();
        Range agg12 = sr12.getAggregations().get("agg");
        for (Range.Bucket entry: agg12.getBuckets()){
            String key = entry.getKeyAsString();
            String fromAsString = (String) entry.getFromAsString();
            String toAsString = (String) entry.getTo();
            long docCount = entry.getDocCount();
            System.out.println("key [{ " + key + " }], from [{ " + fromAsString + " }], to [{ " + toAsString + " }], doc_count [{ " + docCount + " }]");
        }

        //Histogram Aggregation
        AggregationBuilder aggregation13 = AggregationBuilders
                .histogram("agg")
                .field("height")
                .interval(1);
        SearchResponse sr13 = client.prepareSearch().addAggregation(aggregation13).execute().actionGet();
        Histogram agg13 = sr13.getAggregations().get("agg");

        for (Histogram.Bucket entry: agg13.getBuckets()){
            Number key = (Number) entry.getKey();
            long docCount = entry.getDocCount();

            System.out.println("key [{ " + key + " }], doc_count [{ " + docCount + " }]");
        }

        //Date Histogram Aggregation
        AggregationBuilder aggregation14 = AggregationBuilders
                .dateHistogram("agg")
                .field("dateOfBirth")
                .dateHistogramInterval(DateHistogramInterval.YEAR);
        /*AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("dateOfBirth")
                        .dateHistogramInterval(DateHistogramInterval.days(10));*/
        SearchResponse sr14 = client.prepareSearch().addAggregation(aggregation14).execute().actionGet();
        Histogram agg14 = sr14.getAggregations().get("agg");
        for (Histogram.Bucket entry: agg14.getBuckets()){
            DateTime key = (DateTime) entry.getKey();
            String keyAsString = entry.getKeyAsString();
            long docCount = entry.getDocCount();

            System.out.println("key [{ " + keyAsString + " }], date [{ " + key.getYear() + " }], doc_count [{ " + docCount + " }]");
        }

        //Geo Distance Aggregation
        AggregationBuilder aggregation15 = AggregationBuilders
                .geoDistance("agg", new GeoPoint(48.84237171118314,2.33320027692004))
                .field("address.location")
                .unit(DistanceUnit.KILOMETERS)
                .addUnboundedTo(3.0)
                .addRange(3.0, 10.0)
                .addRange(10.0, 500.0);
        SearchResponse sr15 = client.prepareSearch().addAggregation(aggregation15).execute().actionGet();
        Range agg15 = sr15.getAggregations().get("agg");
        for (Range.Bucket entry: agg15.getBuckets()){
            String key = entry.getKeyAsString();
            Number from = (Number) entry.getFrom();
            Number to = (Number) entry.getTo();
            long docCount = entry.getDocCount();

            System.out.println("key [{ " + key + " }], from [{ " + from + " }], to [{ " + to + " }], doc_count [{ " + docCount + " }]");
        }

        //Geo Hash Grid Aggregation
        AggregationBuilder aggregation16 = AggregationBuilders
                .geohashGrid("agg")
                .field("address.location")
                .precision(4);
        SearchResponse sr16 = client.prepareSearch().addAggregation(aggregation16).execute().actionGet();
        GeoHashGrid agg16 = sr16.getAggregations().get("agg");
        for (GeoHashGrid.Bucket entry: agg16.getBuckets()){
            String keyAsString = entry.getKeyAsString();
            GeoPoint key = (GeoPoint) entry.getKey();
            long docCount = entry.getDocCount();

           System.out.println("key [{ " + keyAsString + " }], point { " + key + " }, doc_count [{ " + docCount + " }]");
        }
    }

    /**
     * query和search结合使用
     * @throws Exception
     */
    public void Query() throws Exception{
        //Match All Query
        matchAllQuery();

        //Full text queries
        matchQuery("name", "kimchy elasticsearch");

        multiMatchQuery("kimchy elasticsearch", "user", "message");

        commonTermsQuery("name", "kimchy");

        queryStringQuery("+kimchy -elasticsearch");

        simpleQueryStringQuery("+kimchy -elasticsearch");

        //Term level queries
        termQuery("name", "kimchy");

        termsQuery("tags", "blue", "pill");

        rangeQuery("price")
                .from(5)
                .to(10)
                .includeLower(true)
                .includeUpper(true);

        rangeQuery("age")
                .gte("10")
                .lt("20");

        existsQuery("name");

        prefixQuery("brand", "heine");

        wildcardQuery("user", "k?mch*");

        regexpQuery("name.first", "s.*y");

        fuzzyQuery("name", "kimchy");

        typeQuery("my_type");

        idsQuery("my_type", "type2")
                .addIds("1", "4", "100");

        idsQuery()
                .addIds("1", "4", "100");

        //Compound queries
        constantScoreQuery(termQuery("name", "kimchy")).boost(2.0f);

        boolQuery().must(termQuery("content", "test1"))
                .must(termQuery("content", "test4"))
                .mustNot(termQuery("content", "test2"))
                .should(termQuery("content", "test3"))
                .filter(termQuery("content", "test5"));

        disMaxQuery()
                .add(termQuery("name", "kimchy"))
                .add(termQuery("name", "elasticsearch"))
                .boost(1.2f)
                .tieBreaker(0.7f);

        FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions = {
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        matchQuery("name", "kimchy"),
                        randomFunction()),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        exponentialDecayFunction("age", 0L, 1L)
                )
        };
        functionScoreQuery(functions);

        boostingQuery(
                termQuery("name", "kimchy"),
                termQuery("name", "dadoonet")
        ).negativeBoost(0.2f);

        //Joining queries
        nestedQuery(
                "obj1",
                boolQuery().must(matchQuery("obj1.name", "blue"))
                           .must(rangeQuery("obj1.count").gt(5)),
                ScoreMode.Avg
        );

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 9300)));
        JoinQueryBuilders.hasChildQuery(
                "blog_tag",
                termQuery("tag", "something"),
                ScoreMode.None
        );

        JoinQueryBuilders.hasParentQuery(
                "blog",
                termQuery("tag", "something"),
                false
        );

        //Geo queries 暂时略

        //Specialized queries
        String[] fields = {"name.first", "name.last"};
        String[] texts = {"text like this one"};
        moreLikeThisQuery(fields, texts, null)
                .minTermFreq(1)
                .maxQueryTerms(12);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", 5);
        scriptQuery(new Script(
                ScriptType.STORED,
                null,
                "myscript",
                Collections.singletonMap("param1", 5)
        ));
        scriptQuery(new Script("doc['num1'].value > 1"));

        client.admin().indices().prepareCreate("myIndexName")
                .addMapping("_doc", "query", "type=percolator", "content", "type=text")
                .get();
        QueryBuilder qb = termQuery("content", "amazing");

        client.prepareIndex("myIndexName", "_doc", "myDesignatedQueryName")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("query", qb)
                        .endObject()
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field("content", "This is amazing!");
        docBuilder.endObject();

        //PercolateQueryBuilder percolateQuery = new PercolateQueryBuilder("query", "_doc", BytesReference.toBytes(docBuilder.bytes()));

        String query = "{\"term\": {\"user\":\"kimchy\"}}";
        wrapperQuery(query);

        //Span Term Query
        spanTermQuery("user", "kimchy");

        spanMultiTermQueryBuilder(prefixQuery("user", "ki"));

        spanFirstQuery(spanTermQuery("user", "kimchy"), 3);

        spanNearQuery(spanTermQuery("field", "value1"),12)
                .addClause(spanTermQuery("field", "value2"))
                .addClause(spanTermQuery("field", "value3"))
                .inOrder(false);

        spanOrQuery(spanTermQuery("field", "value1"))
                .addClause(spanTermQuery("field", "valu2"))
                .addClause(spanTermQuery("field", "value3"));

        spanNotQuery(spanTermQuery("field", "value1"),spanTermQuery("field", "value2"));

        spanContainingQuery(
                spanNearQuery(spanTermQuery("field1", "bar"), 5)
                             .addClause(spanTermQuery("field1", "baz"))
                             .inOrder(true),
                spanTermQuery("field1", "foo")
        );

        spanWithinQuery(
                spanNearQuery(spanTermQuery("field1", "bar"), 5)
                             .addClause(spanTermQuery("field1", "baz"))
                             .inOrder(true),
                spanTermQuery("field1", "foo")
        );
    }
}