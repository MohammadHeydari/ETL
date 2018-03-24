package snapp.etl.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import snapp.etl.util.Configuration;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ElasticSearch {
    private static ElasticSearch elasticSearch;
    private TransportClient client;
    private Configuration configuration = Configuration.getInstance();

    private ElasticSearch() {
    }

    public static ElasticSearch connect() {
        if (elasticSearch == null) {
            elasticSearch = new ElasticSearch();
            try {
                elasticSearch.initConnection();
                return elasticSearch;
            } catch (UnknownHostException e) {
                System.out.println("Could not connect to ElasticSearch Cluster");
                System.exit(0);
                return null;
            }
        } else {
            return elasticSearch;
        }
    }

    public static Map<String, Object> getJsonAsMap(String jsonObject) { //todo: the input parameter is to baaaaad
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Gson gson = new Gson();
        Map<String, Object> myMap = gson.fromJson(jsonObject, type);
        return myMap;
    }

    public static Map<String, Object> generateErrorMessage(String zone, String message) {
        return getJsonAsMap(String.format("{\"type\":\"error\" , \"zone\":\"%s\" , \"message\":\"%s\"}", zone, message));
    }

    public IndexRequestBuilder getIndexObject(String action) {
        String pattern = "";
        String prefix = "";
        if (action.equals("event")) {
            pattern = configuration.getConfig("elastic.event.index.postfix");
            prefix = configuration.getConfig("elastic.event.index.prefix");

        } else if (action.equals("location")) {
            pattern = configuration.getConfig("elastic.location.index.postfix");
            prefix = configuration.getConfig("elastic.location.index.prefix");
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        String postfix = simpleDateFormat.format(new Date());

        return client.prepareIndex(prefix + postfix, "snapp_app_data");
    }

    public BulkRequestBuilder getBulkObject() {
        return client.prepareBulk();
    }

    private void initConnection() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", configuration.getConfig("elastic.cluster.name")).build();
        String[] split = configuration.getConfig("elastic.cluster.node").split(",");
        client = new PreBuiltTransportClient(settings);
        for (int i = 0; i < split.length; i++) {
            String nodeName = split[i];
            client.addTransportAddress(new TransportAddress(InetAddress.getByName(nodeName), 9300));
        }   //Might have better API. But I used the most simple way

    }

}
