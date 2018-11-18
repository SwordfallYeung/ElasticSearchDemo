package com.swordfall.elasticsearch;

import javafx.beans.property.adapter.ReadOnlyJavaBeanBooleanProperty;
import org.apache.http.HttpHost;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.rankeval.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.lang.reflect.MalformedParameterizedTypeException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * @author: Swordfall Yeung
 * @date:
 * @desc:
 */
public class RestClientUtils {

    private RestHighLevelClient client = null;
    private RestClient restClient = null;

    public RestClientUtils() {
        if (client == null){
            synchronized (RestHighLevelClient.class){
                if (client == null){
                    client = getClient();
                }
            }
        }
    }

    private RestHighLevelClient getClient(){
        RestHighLevelClient client = null;

        try {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("192.168.187.201", 9300, "http")
                    )
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    private RestClient getRestClient(){
        RestClient client = null;

        try {
            client = RestClient.builder(
                            new HttpHost("192.168.187.201", 9300, "http")
                    ).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    public void closeClient(){
        try {
            if (client != null){
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*------------------------------------------------ document Api start --------------------------------------------*/

    /**
     * 增，插入记录
     * @throws Exception
     */
    public void index() throws Exception{
        //String
        IndexRequest request = new IndexRequest(
                "posts",
                "doc",
                "1"
        );
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        //Map
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest("posts", "doc", "1").source(jsonMap);

        //XContentBuilder automatically converted to JSON
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate" , new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest1 = new IndexRequest("posts", "doc", "1")
                .source(builder);

        //source -> key-pairs
        IndexRequest indexRequest2 = new IndexRequest("posts", "doc", "1")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch"
                );

        //Optional arguments
        request.routing("routing");

        request.parent("parent");

        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");

        request.version(2);

        request.versionType(VersionType.EXTERNAL);

        request.opType(DocWriteRequest.OpType.CREATE);
        request.opType("create");

        request.setPipeline("pipeline");

        //Synchronous Execution
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.indexAsync(request, RequestOptions.DEFAULT, listener);

        //Index Response
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }

        //throw Exception
        IndexRequest request1 = new IndexRequest("posts", "doc", "1")
                .source("field", "value")
                .version(1);
        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT){

            }
        }

        //in case opType throw Exception
        IndexRequest request2 = new IndexRequest("posts", "doc", "1")
                .source("field", "value")
                .opType(DocWriteRequest.OpType.CREATE);
        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT){

            }
        }
    }

    /**
     * 根据 id 获取数据
     * @throws Exception
     */
    public void get() throws Exception{
        GetRequest request = new GetRequest("posts", "doc", "1");

        //optional arguments
        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        //specific fields
        String[] includes1 = new String[]{"message", "*Date"};
        String[] excludes1 = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext1 = new FetchSourceContext(true, includes1, excludes1);
        request.fetchSourceContext(fetchSourceContext1);

        //source exclusion for specific fields
        request.storedFields("message");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String message = response.getField("message").getValue();

        request.routing("routing");
        request.parent("parent");
        request.preference("preference");
        request.realtime(false);
        request.refresh(true);
        request.version(2);
        request.versionType(VersionType.EXTERNAL);

        //Synchronous Execution
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.getAsync(request, RequestOptions.DEFAULT, listener);

        //Get Response
        String index = getResponse.getIndex();
        String type = getResponse.getType();
        String id = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
        } else {

        }

        //throw Exception
        GetRequest request1 = new GetRequest("does_not_exist", "doc", "1");
        try {
            GetResponse getResponse1 = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {

            }
        }

        //version
        try {
            GetRequest request2 = new GetRequest("posts", "doc", "1").version(2);
            GetResponse getResponse2 = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {

            }
        }
    }

    /**
     * 存在
     * @throws Exception
     */
    public void exists() throws Exception{
        GetRequest getRequest = new GetRequest("posts", "doc", "1");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        //Synchronous Execution
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean exists) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.existsAsync(getRequest, RequestOptions.DEFAULT, listener);
    }

    public void delete() throws Exception{
        DeleteRequest request = new DeleteRequest("posts", "doc", "1");

        //optional arguments
        request.routing("routing");
        request.parent("parent");
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        request.version(2);
        request.versionType(VersionType.EXTERNAL);

        //Synchronous Execution
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.deleteAsync(request, RequestOptions.DEFAULT, listener);

        //Delete Response
        String index = deleteResponse.getIndex();
        String type = deleteResponse.getType();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }

        // document was not found
        DeleteRequest request1 = new DeleteRequest("posts", "doc", "does_not_exist");
        DeleteResponse deleteResponse1 = client.delete(request1, RequestOptions.DEFAULT);
        if (deleteResponse1.getResult() == DocWriteResponse.Result.NOT_FOUND) {

        }

        //throw Exception
        try {
            DeleteRequest request2 = new DeleteRequest("posts", "doc", "1").version(2);
            DeleteResponse deleteResponse2 = client.delete(request2, RequestOptions.DEFAULT);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {

            }
        }
    }

    public void update() throws Exception{
        UpdateRequest request = new UpdateRequest("posts", "doc", "1");
        Map<String, Object> parameters = Collections.singletonMap("count", 4);

        //inline script
        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.field += params.count", parameters);
        request.script(inline);

        //stored script
        Script stored = new Script(ScriptType.STORED, null, "increment-field", parameters);
        request.script(stored);

        //partial document String
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update\"" +
                "}";
        request.doc(jsonString, XContentType.JSON);

        //partial document Map
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated", new Date());
        jsonMap.put("reason", "daily update");
        request.doc(jsonMap);

        //partial document XContentBuilder
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.timeField("updated", new Date());
            builder.field("reason", "daily update");
        }
        builder.endObject();
        request.doc(builder);

        //partial document Object key-pairs
        request.doc("updated", new Date(),
                   "reason", "daily update");

        //upserts
        String jsonString1 = "{\"created\":\"2017-01-01\"}";
        request.upsert(jsonString1, XContentType.JSON);

        //optional arguments
        request.routing("routing");
        request.parent("parent");
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        request.retryOnConflict(3);
        request.fetchSource(true);

        //Enable source retrieval
        String[] includes = new String[]{"updated", "r*"};
        String[] excludes = Strings.EMPTY_ARRAY;
        request.fetchSource(new FetchSourceContext(true, includes, excludes));

        //specific fields
        String[] includes1 = Strings.EMPTY_ARRAY;
        String[] excludes1 = new String[]{"updated"};
        request.fetchSource(new FetchSourceContext(true, includes1, excludes1));

        request.version(2);
        request.detectNoop(false);

        request.scriptedUpsert(true);
        request.docAsUpsert(true);

        request.waitForActiveShards(2);
        request.waitForActiveShards(ActiveShardCount.ALL);

        //Synchronous Execution
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.updateAsync(request, RequestOptions.DEFAULT, listener);

        //update Response
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }

        GetResult result = updateResponse.getGetResult();
        if (result.isExists()) {
            String sourceAsString = result.sourceAsString();
            Map<String, Object> sourceAsMap = result.sourceAsMap();
            byte[] sourceAsBytes = result.source();
        } else {

        }

        //check for shard failures
        ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }

        //throw Exception
        UpdateRequest request1 = new UpdateRequest("posts", "type", "does_not_exist")
                .doc("field", "value");
        try {
            UpdateResponse updateResponse1 = client.update(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {

            }
        }

        //throw Exception version conflict
        UpdateRequest request2 = new UpdateRequest("posts", "doc", "1")
                .doc("field", "value")
                .version(1);
        try {
            UpdateResponse updateResponse2 = client.update(request, RequestOptions.DEFAULT);
        } catch(ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {

            }
        }
    }

    public void bulk() throws Exception{
        BulkRequest request = new BulkRequest();
        //Index
        request.add(new IndexRequest("posts", "doc", "1")
                    .source(XContentType.JSON, "field", "foo"));
        request.add(new IndexRequest("posts", "doc", "2")
                .source(XContentType.JSON, "field", "bar"));
        request.add(new IndexRequest("posts", "doc", "3")
                .source(XContentType.JSON, "field", "baz"));

        //Other
        request.add(new DeleteRequest("posts", "doc", "3"));
        request.add(new UpdateRequest("posts", "doc", "2")
                .doc(XContentType.JSON, "other", "test"));
        request.add(new IndexRequest("posts", "doc", "4")
                .source(XContentType.JSON, "field", "baz"));

        //optional arguments
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        request.waitForActiveShards(2);
        request.waitForActiveShards(ActiveShardCount.ALL);

        //Synchronous Execution
        BulkResponse bulkResponses = client.bulk(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.bulkAsync(request, RequestOptions.DEFAULT, listener);

        //Bulk Response
        for (BulkItemResponse bulkItemResponse: bulkResponses){
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;

            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;

            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
            }
        }

        //failures
        if (bulkResponses.hasFailures()){

        }

        for (BulkItemResponse bulkItemResponse : bulkResponses) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();

            }
        }

        //Bulk Processor
        BulkProcessor.Listener listener1 = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request1, bulkListener) -> client.bulkAsync(request1, RequestOptions.DEFAULT, bulkListener);
        BulkProcessor bulkProcessor = BulkProcessor.builder(bulkConsumer, listener1).build();

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer1 =
                (request2, bulkListener) -> client.bulkAsync(request2, RequestOptions.DEFAULT, bulkListener);
        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer1, listener1);
        builder.setBulkActions(500);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(0);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));

        //Once the BulkProcessor is created requests can be added to it:
        IndexRequest one = new IndexRequest("posts", "doc", "1").
                source(XContentType.JSON, "title",
                        "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts", "doc", "2")
                .source(XContentType.JSON, "title",
                        "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts", "doc", "3")
                .source(XContentType.JSON, "title",
                        "The Future of Federated Search in Elasticsearch");
        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);

        //the listener provides methods to access to the BulkRequest and the BulkResponse:
        BulkProcessor.Listener listener2 = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                String s = String.format("Executing bulk [{}] with {} requests",
                        executionId, numberOfActions);
                System.out.println(s);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    String s = String.format("Bulk [{}] executed with failures", executionId);
                    System.out.println(s);
                } else {
                    String s = String.format("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());
                    System.out.println(s);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                String s = String.format("Failed to execute bulk", failure);
                System.out.println(s);
            }
        };

        boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);

    }

    public void multiGet() throws Exception{
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item(
                "index",
                "type",
                "example_id"
        ));
        request.add(new MultiGetRequest.Item("index", "type", "another_id"));

        //optional arguments
        request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));

        String[] includes = new String[] {"foo", "*r"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .fetchSourceContext(fetchSourceContext));

        String[] includes1 = Strings.EMPTY_ARRAY;
        String[] excludes1 = new String[] {"foo", "*r"};
        FetchSourceContext fetchSourceContext1 =
                new FetchSourceContext(true, includes1, excludes1);
        request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .fetchSourceContext(fetchSourceContext));

        request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .storedFields("foo"));
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        MultiGetItemResponse item = response.getResponses()[0];
        String value = item.getResponse().getField("foo").getValue();

        request.add(new MultiGetRequest.Item("index", "type", "with_routing")
                .routing("some_routing"));
        request.add(new MultiGetRequest.Item("index", "type", "with_parent")
                .parent("some_parent"));
        request.add(new MultiGetRequest.Item("index", "type", "with_version")
                .versionType(VersionType.EXTERNAL)
                .version(10123L));

        request.preference("some_preference");
        request.realtime(false);
        request.refresh(true);

        //Synchronous Execution
        MultiGetResponse responses = client.mget(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<MultiGetResponse> listener = new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse response) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.mgetAsync(request, RequestOptions.DEFAULT, listener);

        //Multi Get Response
        MultiGetItemResponse firstItem = response.getResponses()[0];
        GetResponse firstGet = firstItem.getResponse();
        String index = firstItem.getIndex();
        String type = firstItem.getType();
        String id = firstItem.getId();
        if (firstGet.isExists()) {
            long version = firstGet.getVersion();
            String sourceAsString = firstGet.getSourceAsString();
            Map<String, Object> sourceAsMap = firstGet.getSourceAsMap();
            byte[] sourceAsBytes = firstGet.getSourceAsBytes();
        } else {

        }

        MultiGetRequest request2 = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .version(1000L));
        MultiGetResponse response2 = client.mget(request, RequestOptions.DEFAULT);
        MultiGetItemResponse item2 = response.getResponses()[0];
        Exception e = item.getFailure().getFailure();
        ElasticsearchException ee = (ElasticsearchException) e;



    }

    /*------------------------------------------------ document Api end ----------------------------------------------*/

    /*------------------------------------------------ search Api 多条件查询 start ----------------------------------------------*/

    public void search() throws Exception{
        //match all query
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        //optional arguments
        SearchRequest searchRequest1 = new SearchRequest("posts");
        searchRequest1.types("doc");
        searchRequest1.routing("routing");
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.preference("_local");

        //using the SearchSourceBuilder
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));;
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest searchRequest2 = new SearchRequest();
        //index 数据库
        searchRequest2.indices("posts");
        searchRequest2.source(sourceBuilder);

        //Building queries
        //One way, QueryBuilder can be created using its constructor
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("user", "kimchy");
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        matchQueryBuilder.prefixLength(3);
        matchQueryBuilder.maxExpansions(10);
        //Two way, QueryBuilder objects can also be created using the QueryBuilders utility class.
        QueryBuilder matchQueryBuilder1 = matchQuery("user", "kimchy")
                                                       .fuzziness(Fuzziness.AUTO)
                                                       .prefixLength(3)
                                                       .maxExpansions(10);

        searchSourceBuilder.query(matchQueryBuilder1);

        //Specifying Sorting
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.sort(new FieldSortBuilder("_uid").order(SortOrder.ASC));

        //Source filtering, turn off _source retrieval completely
        sourceBuilder.fetchSource(false);
        //an array of one or more wildcard patterns to control which fields get included or excluded in a more fine grained way
        String[] includeFields = new String[] {"title", "user", "innerObject.*"};
        String[] excludeFields = new String[] {"_type"};
        sourceBuilder.fetchSource(includeFields, excludeFields);

        //Requesting Highlighting
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitile = new HighlightBuilder.Field("title");
        highlightTitile.highlighterType("unified");
        highlightBuilder.field(highlightTitile);

        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        highlightBuilder.field(highlightUser);
        searchSourceBuilder1.highlighter(highlightBuilder);

        //Requesting Aggregations
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                .field("company.keyword");
        aggregation.subAggregation(AggregationBuilders.avg("average_age")
                .field("age"));
        searchSourceBuilder2.aggregation(aggregation);

        //Requesting Suggestions
        SearchSourceBuilder searchSourceBuilder3 = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("user").text("kmichy");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
        searchSourceBuilder3.suggest(suggestBuilder);

        //Profiling Queries and Aggregations
        SearchSourceBuilder searchSourceBuilder4 = new SearchSourceBuilder();
        searchSourceBuilder4.profile(true);

        //Synchronous Execution
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);

        //SearchResponse
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }

        //Retrieving SearchHits
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String documentTitle = (String) sourceAsMap.get("title");
            List<Object> users = (List<Object>) sourceAsMap.get("user");
            Map<String, Object> innerObject =
                    (Map<String, Object>) sourceAsMap.get("innerObject");
        }

        //Retrieving Highlighting
        SearchHits hits1 = searchResponse.getHits();
        for (SearchHit hit : hits1.getHits()) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlight = highlightFields.get("title");
            Text[] fragments = highlight.fragments();
            String fragmentString = fragments[0].string();
        }

        //Retrieving Aggregations
        Aggregations aggregations = searchResponse.getAggregations();
        Terms byCompanyAggregation = aggregations.get("by_company");
        Terms.Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
        Avg averageAge = elasticBucket.getAggregations().get("average_age");
        double avg = averageAge.getValue();

        Range range = aggregations.get("by_company");
        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        Terms companyAggregation = (Terms) aggregationMap.get("by_company");

        List<Aggregation> aggregationList = aggregations.asList();
        for (Aggregation agg : aggregations) {
            String type = agg.getType();
            if (type.equals(TermsAggregationBuilder.NAME)) {
                Terms.Bucket elasticBucket1 = ((Terms) agg).getBucketByKey("Elastic");
                long numberOfDocs = elasticBucket1.getDocCount();
            }
        }

        //Retrieving Suggestions
        Suggest suggest = searchResponse.getSuggest();
        TermSuggestion termSuggestion = suggest.getSuggestion("suggest_user");
        for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
            for (TermSuggestion.Entry.Option option : entry) {
                String suggestText = option.getText().string();
            }
        }

        //Retrieving Profiling Results
        Map<String, ProfileShardResult> profilingResults =
                searchResponse.getProfileResults();
        for (Map.Entry<String, ProfileShardResult> profilingResult : profilingResults.entrySet()) {
            String key = profilingResult.getKey();
            ProfileShardResult profileShardResult = profilingResult.getValue();
        }
    }

    public void searchScroll() throws Exception{
        //Initialize the search scroll context
        SearchRequest searchRequest = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQuery("title", "Elasticsearch"));
        searchSourceBuilder.size(10);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHits hits = searchResponse.getHits();

        //Retrieve all the relevant documents
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(30));
        SearchResponse searchResponse1 = client.scroll(scrollRequest, RequestOptions.DEFAULT);
        scrollId = searchResponse1.getScrollId();
        hits = searchResponse1.getHits();

        //optional arguments
        scrollRequest.scroll(TimeValue.timeValueSeconds(60L));
        scrollRequest.scroll("60s");

        //Synchronous Execution
        SearchResponse searchResponse2 = client.scroll(scrollRequest, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<SearchResponse> scrollListener =
                new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {

                    }

                    @Override
                    public void onFailure(Exception e) {

                    }
                };
        client.scrollAsync(scrollRequest, RequestOptions.DEFAULT, scrollListener);


        //Full example
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest1 = new SearchRequest("posts");
        searchRequest1.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.query(matchQuery("title", "Elasticsearch"));
        searchRequest.source(searchSourceBuilder1);

        SearchResponse searchResponse3 = client.search(searchRequest1, RequestOptions.DEFAULT);
        String scrollId1 = searchResponse3.getScrollId();
        SearchHit[] searchHits = searchResponse3.getHits().getHits();

        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest2 = new SearchScrollRequest(scrollId);
            scrollRequest2.scroll(scroll);
            searchResponse = client.scroll(scrollRequest2, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();

        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
    }

    public void multiSearch() throws Exception{
        MultiSearchRequest request = new MultiSearchRequest();
        SearchRequest firstSearchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "kimchy"));
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);

        SearchRequest secondSearchRequest = new SearchRequest();
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "luca"));
        secondSearchRequest.source(searchSourceBuilder);
        request.add(secondSearchRequest);

        //optional arguments
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.types("doc");

        //Synchronous Execution
        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<MultiSearchResponse> listener = new ActionListener<MultiSearchResponse>() {
            @Override
            public void onResponse(MultiSearchResponse response) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.msearchAsync(request, RequestOptions.DEFAULT, listener);

        //MultiSearchResponse
        MultiSearchResponse.Item firstResponse = response.getResponses()[0];
        SearchResponse searchResponse = firstResponse.getResponse();
        MultiSearchResponse.Item secondResponse = response.getResponses()[1];
        searchResponse = secondResponse.getResponse();
    }

    public void searchTemplate() throws Exception{
        //Inline Templates
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest("posts"));

        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                "{" +
                 "  \"query\": { \"match\": { \"{{ field }}\": \"{{ value }}\" } }," +
                 "  \"size\": \"{{ size }}\"" +
                 "}");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "title");
        scriptParams.put("value", "elasticsearch");
        scriptParams.put("size", 5);
        request.setScriptParams(scriptParams);

        //Registered Templates
        Request scriptRequest = new Request("POST", "_scripts/title_search");
        scriptRequest.setJsonEntity(
                "{" +
                "  \"script\": {" +
                "        \"lang\": \"mustache\"," +
                "        \"source\": {" +
                "             \"query\": { \"match\": { \"{{field}}\": \"{{value}}\" } }," +
                "             \"size\": \"{{size}}\"" +
                "          }" +
                "     }" +
                "}"
        );
        Response scriptResponse = getRestClient().performRequest(scriptRequest);

        //instead of providing an inline script
        SearchTemplateRequest request1 = new SearchTemplateRequest();
        request1.setRequest(new SearchRequest("posts"));
        request1.setScriptType(ScriptType.STORED);
        request1.setScript("title_search");
        Map<String, Object> params = new HashMap<>();
        params.put("field", "title");
        params.put("value", "elasticsearch");
        params.put("size", 5);
        request1.setScriptParams(params);

        //Rendering Templates
        request1.setSimulate(true);

        //Optional Arguments
        request1.setExplain(true);
        request1.setProfile(true);

        //Synchronous Execution
        SearchTemplateResponse response = client.searchTemplate(request1, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<SearchTemplateResponse> listener = new ActionListener<SearchTemplateResponse>() {
            @Override
            public void onResponse(SearchTemplateResponse response) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.searchTemplateAsync(request1, RequestOptions.DEFAULT, listener);

        //SearchTemplate Response
        SearchTemplateResponse response1 = client.searchTemplate(request1, RequestOptions.DEFAULT);
        SearchResponse searchResponse = response1.getResponse();
        BytesReference source = response1.getSource();
    }

    public void MultiSearchTemplate() throws Exception{
        String[] searchTerms = {"elasticsearch", "logstash", "kibana"};
        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        for (String searchTerm: searchTerms){
            SearchTemplateRequest request = new SearchTemplateRequest();
            request.setRequest(new SearchRequest("posts"));

            request.setScriptType(ScriptType.INLINE);
            request.setScript(
                    "{" +
                    " \"query\": { \"match\": { \"{{field}}\": \"{{value}}\" }}," +
                    " \"size\": \"{{size}}\"" +
                    "}"
            );

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "title");
            scriptParams.put("value", searchTerm);
            scriptParams.put("size", 5);
            request.setScriptParams(scriptParams);

            multiRequest.add(request);

            //Optional arguments

            //Synchronous Execution
            MultiSearchTemplateResponse multiResponse = client.msearchTemplate(multiRequest, RequestOptions.DEFAULT);

            //Asynchronous Execution
            ActionListener<MultiSearchTemplateResponse> listener = new ActionListener<MultiSearchTemplateResponse>() {
                @Override
                public void onResponse(MultiSearchTemplateResponse response) {

                }

                @Override
                public void onFailure(Exception e) {

                }
            };
            client.msearchTemplateAsync(multiRequest, RequestOptions.DEFAULT, listener);

            //MultiSearchTemplateResponse
            for (MultiSearchTemplateResponse.Item item : multiResponse.getResponses()) {
                if (item.isFailure()) {
                    String error = item.getFailureMessage();
                } else {
                    SearchTemplateResponse searchTemplateResponse = item.getResponse();
                    SearchResponse searchResponse = searchTemplateResponse.getResponse();
                    searchResponse.getHits();
                }
            }
        }
    }

    public void FieldCapabilities() throws Exception{
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
                .fields("user")
                .indices("posts", "authors", "contributors");

        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        //Synchronous Execution
        FieldCapabilitiesResponse response = client.fieldCaps(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<FieldCapabilitiesResponse> listener = new ActionListener<FieldCapabilitiesResponse>() {
            @Override
            public void onResponse(FieldCapabilitiesResponse response) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.fieldCapsAsync(request, RequestOptions.DEFAULT, listener);

        //FieldCapabilitiesResponse
        Map<String, FieldCapabilities> userResponse = response.getField("user");
        FieldCapabilities textCapabilities = userResponse.get("keyword");

        boolean isSearchable = textCapabilities.isSearchable();
        boolean isAggregatable = textCapabilities.isAggregatable();

        String[] indices = textCapabilities.indices();
        String[] nonSearchableIndices = textCapabilities.nonSearchableIndices();
        String[] nonAggregatableIndices = textCapabilities.nonAggregatableIndices();//


    }

    public void RankingEvaluation() throws Exception{
        EvaluationMetric metric = new PrecisionAtK();
        List<RatedDocument> rateDocs = new ArrayList<>();
        rateDocs.add(new RatedDocument("posts", "1", 1));
        SearchSourceBuilder searchQuery = new SearchSourceBuilder();
        searchQuery.query(QueryBuilders.matchQuery("user", "kimchy"));
        RatedRequest ratedRequest = new RatedRequest("kimchy_query", rateDocs, searchQuery);
        List<RatedRequest> ratedRequests = Arrays.asList(ratedRequest);
        RankEvalSpec specification = new RankEvalSpec(ratedRequests, metric);
        RankEvalRequest request = new RankEvalRequest(specification, new String[]{ "posts" });

        //Synchronous Execution
        RankEvalResponse response = client.rankEval(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<RankEvalResponse> listener = new ActionListener<RankEvalResponse>() {
            @Override
            public void onResponse(RankEvalResponse response) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.rankEvalAsync(request, RequestOptions.DEFAULT, listener);

        double evaluationResult = response.getMetricScore();
        Map<String, EvalQueryQuality> partialResults =
                response.getPartialResults();
        EvalQueryQuality evalQuality =
                partialResults.get("kimchy_query");
        double qualityLevel = evalQuality.metricScore();
        List<RatedSearchHit> hitsAndRatings = evalQuality.getHitsAndRatings();
        RatedSearchHit ratedSearchHit = hitsAndRatings.get(0);
        MetricDetail metricDetails = evalQuality.getMetricDetails();
        String metricName = metricDetails.getMetricName();
        PrecisionAtK.Detail detail = (PrecisionAtK.Detail) metricDetails;
    }

    public void Explain() throws Exception{
        ExplainRequest request = new ExplainRequest("contributors", "doc", "1");
        request.query(QueryBuilders.termQuery("user", "tanguy"));

        //optional arguments
        request.routing("routing");
        request.preference("_local");
        request.fetchSourceContext(new FetchSourceContext(true, new String[]{"user"}, null));

        request.storedFields(new String[]{"user"});

        //Synchronous Execution
        ExplainResponse response = client.explain(request, RequestOptions.DEFAULT);

        //Asynchronous Execution
        ActionListener<ExplainResponse> listener = new ActionListener<ExplainResponse>() {
            @Override
            public void onResponse(ExplainResponse explainResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.explainAsync(request, RequestOptions.DEFAULT, listener);

        //ExplainResponse
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        boolean exists = response.isExists();
        boolean match = response.isMatch();
        boolean hasExplanation = response.hasExplanation();
        Explanation explanation = response.getExplanation();
        GetResult getResult = response.getGetResult();

        Map<String, Object> source = getResult.getSource();
        Map<String, DocumentField> fields = getResult.getFields();
    }
    /*------------------------------------------------ search Api 多条件查询 end ----------------------------------------------*/
}
