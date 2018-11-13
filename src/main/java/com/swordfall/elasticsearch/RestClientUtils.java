package com.swordfall.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.lang.reflect.MalformedParameterizedTypeException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: Swordfall Yeung
 * @date:
 * @desc:
 */
public class RestClientUtils {

    private RestHighLevelClient client = null;

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

    /*------------------------------------------------ document Api end ----------------------------------------------*/

}
