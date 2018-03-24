package snapp.etl.ingest;

import com.google.gson.JsonObject;
import snapp.etl.db.ElasticSearch;
import snapp.etl.db.MySQLUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class PassengerRideRequestIngestor extends Ingestor {
    public static Connection sqlConnection;

    public PassengerRideRequestIngestor() {
        try {
            if (sqlConnection == null) {
                sqlConnection = MySQLUtil.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    @Override
    protected Map<String, Object> enrichEngine(JsonObject message) throws Exception {
        Statement statement = sqlConnection.createStatement();
        String destination_lat = message.get("destination_lat").toString();
        String destination_lng = message.get("destination_lng").toString();
        String origin_lat = message.get("origin_lat").toString();
        String origin_lng = message.get("origin_lng").toString();
        String query = "select *  from geofence where  GEOGRAPHY_CONTAINS(area,\"POINT(%s %s)\") =1\n";
        ResultSet resultSet = statement.executeQuery(String.format(query, destination_lng, destination_lat));
        String combine = "";
        if (resultSet != null) {
            if (resultSet.next()) {
                combine = resultSet.getString("fence_name");
                message.addProperty("destination_name", resultSet.getString("fence_name"));
                message.addProperty("destination_id", resultSet.getString("fence_id"));
                message.addProperty("destination_district", resultSet.getString("district"));

            } else {
                message.addProperty("destination_name", "not_defined");
                message.addProperty("destination_id", "not_defined");
                message.addProperty("destination_district", "not_defined");
            }

        }
        resultSet = statement.executeQuery(String.format(query, origin_lng, origin_lat));
        if (resultSet != null) {
            if (resultSet.next()) {
                combine = combine + "-" + resultSet.getString("fence_name");
                message.addProperty("origin_name", resultSet.getString("fence_name"));
                message.addProperty("origin_id", resultSet.getString("fence_id"));
                message.addProperty("origin_district", resultSet.getString("district"));
            } else {
                message.addProperty("origin_name", "not_defined");
                message.addProperty("origin_id", "not_defined");
                message.addProperty("origin_district", "not_defined");
            }

        }
        message.addProperty("combine_address", combine);

        statement.close();
        return ElasticSearch.getJsonAsMap(message.toString());
    }

    @Override
    protected Map<String, Object> exceptionHandler() {
        System.out.println("Error in passenger_Ride_Request");
        return ElasticSearch.generateErrorMessage("PassengerRideRequest", "Module Internal Error");
    }
}
