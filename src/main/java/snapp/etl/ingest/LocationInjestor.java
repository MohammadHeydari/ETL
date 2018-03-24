package snapp.etl.ingest;

import com.google.gson.JsonObject;
import snapp.etl.db.ElasticSearch;

import java.util.Map;

public class LocationInjestor extends Ingestor {
    @Override
    protected Map<String, Object> enrichEngine(JsonObject message) throws Exception {
        JsonObject outputJsonObject = new JsonObject();
        outputJsonObject.addProperty("location", String.format("%s , %s",
                message.get("lt").getAsFloat(), message.get("lg").getAsFloat()));
        outputJsonObject.addProperty("ts", message.get("@timestamp").getAsString());
        outputJsonObject.addProperty("hostname", message.get("host").getAsString());
        outputJsonObject.addProperty("driver_id", message.get("i").getAsBigInteger());
        outputJsonObject.addProperty("sp", message.get("sp").getAsBigInteger());
        outputJsonObject.addProperty("st", message.get("st").getAsBigInteger());
        return ElasticSearch.getJsonAsMap(outputJsonObject.toString());
    }

    @Override
    protected Map<String, Object> exceptionHandler() {
        return null;
    }
}
