package snapp.etl.router;

import com.google.gson.JsonObject;
import snapp.etl.db.ElasticSearch;
import snapp.etl.ingest.Ingestor;
import snapp.etl.ingest.LocationInjestor;
import snapp.etl.ingest.PassengerRideRequestIngestor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class EventRouter {
    private static JsonObject eventGeneralNormalization(String eventName, JsonObject jsonObject) {
        jsonObject.addProperty("app_event", eventName);
        return jsonObject;
    }


    public static List<Map<String, Object>> route(String scopeName, JsonObject jsonObject) {
        Ingestor ingestor = null;
        JsonObject NormalizedJsonObject = null;
        String eventName = "";
        ArrayList returnList = new ArrayList<Map<String, Object>>();
        if (scopeName.equals("EVENT")) {
            eventName = jsonObject.get("_event").getAsString();
            NormalizedJsonObject = eventGeneralNormalization(eventName, jsonObject);
        } else if (scopeName.equals("LOCATION")) {
            eventName = scopeName;
            NormalizedJsonObject = jsonObject;
        }

        switch (eventName.toUpperCase()) {
            case "PASSENGER_RIDE_REQUEST": {
                ingestor = new PassengerRideRequestIngestor();
                returnList.add(ingestor.getEnrichedMessage(NormalizedJsonObject));
                return returnList;
            }

            case "DRIVER_GET_SYSTEM_MESSAGE": {
                return returnList;
            }

            case "LOCATION": {
                ingestor = new LocationInjestor();
                returnList.add(ingestor.getEnrichedMessage(NormalizedJsonObject));
                return returnList;
            }

            default: {
                returnList.add(ElasticSearch.getJsonAsMap(NormalizedJsonObject.toString()));
                return returnList;
            }
        }
    }

}
