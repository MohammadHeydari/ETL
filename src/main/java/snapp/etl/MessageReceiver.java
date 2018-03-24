package snapp.etl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import snapp.etl.db.ElasticSearch;
import snapp.etl.router.CombinerRouter;
import snapp.etl.router.EventRouter;
import snapp.etl.util.Configuration;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MessageReceiver implements Runnable {
    private Iterator<ConsumerRecord<String, String>> bulk;
    private ElasticSearch ESClient;
    private String action;

    public MessageReceiver(ElasticSearch client, String brokerName, Iterator<ConsumerRecord<String, String>> iterator) {
        bulk = iterator;
        ESClient = client;
        action = brokerName;
    }

    public void run() {
        BulkRequestBuilder bulkRequest = ESClient.getBulkObject();
        IndexRequestBuilder indexObject = ESClient.getIndexObject(action);
        CombinerRouter combinerRouter = new CombinerRouter();
        Configuration configuration = Configuration.getInstance();
        while (bulk.hasNext()) {
            ConsumerRecord<String, String> message = bulk.next();
            JsonParser jsonParser = new JsonParser();
            JsonObject row = jsonParser.parse(message.value()).getAsJsonObject();
            //move to routers
            List<Map<String, Object>> route = EventRouter.route(action.toUpperCase(), row);
            if (configuration.getConfig("repository.activation").equals("ENABLE"))
                combinerRouter.addRow(action.toUpperCase(), row);
            for (int i = 0; i < route.size(); i++) {
                if (route.get(i) != null) {
                    IndexRequestBuilder requestBuilder = indexObject.setSource(route.get(i));
                    bulkRequest.add(requestBuilder);
                }
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = bulkRequest.get();
            Date date = new Date();
            combinerRouter.flushRows();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
                System.out.println("================================");
            } else {
                System.out.println(String.format("Data Inserted Into Index %s at %s", action.toUpperCase(), date.toString()));
            }
        }
    }
}
