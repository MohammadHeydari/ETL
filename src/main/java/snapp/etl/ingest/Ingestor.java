package snapp.etl.ingest;

import com.google.gson.JsonObject;

import java.util.Map;

public abstract class Ingestor {

    protected abstract Map<String, Object> enrichEngine(JsonObject message) throws Exception;

    protected abstract Map<String, Object> exceptionHandler();

    public Map<String, Object> getEnrichedMessage(JsonObject message) {
        try {
            return enrichEngine(message);
        } catch (Exception e) {
            e.printStackTrace();
            return exceptionHandler();
        }
    }

}
