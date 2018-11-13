package com.swordfall.elasticsearch;

/**
 * @Author: Yang JianQiu
 * @Date: 2018/11/12 11:02
 */
public class ElasticSearchDemo {

    public static void main(String[] args) throws Exception{
        TransportClientUtils client = new TransportClientUtils();

        /*String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        client.index(json);*/

        /*String id = "M5FJB2cBi36MQkKOoNek";
        client.get(id);*/

        /*String id = "M5FJB2cBi36MQkKOoNek";
        client.delete(id);*/

       /* client.deleteByQueryAPI();*/

        client.deleteByQueryAPIWithListener();
    }


}