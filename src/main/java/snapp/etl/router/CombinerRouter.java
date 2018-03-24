package snapp.etl.router;

import com.google.gson.JsonObject;
import snapp.etl.db.MySQLUtil;
import snapp.etl.util.Configuration;

import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class CombinerRouter {
    private static SimpleDateFormat outputDateFormat;
    private static List<String> eventList;
    StringBuffer rows = new StringBuffer();


    public CombinerRouter() {
        outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String events[] = Configuration.getInstance().getConfig("ride.combiner.event.name").split(",");
        eventList = Arrays.asList(events);
    }

    public void addRow(String scopeName, JsonObject eventBody) {
        if (scopeName.equals("EVENT")) {
            String eventName = eventBody.get("app_event").getAsString();
            if (eventList.contains(eventName.toUpperCase())) {
                try {
                    String driverId = eventBody.get("ride_id").getAsString();
                    Date timeStamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+03:30'").parse(eventBody.get("_ets").getAsString());
                    rows.append(String.format(",('%s','%s','%s','%s')",
                            driverId,
                            eventName,
                            outputDateFormat.format(timeStamp),
                            eventBody));
                } catch (Exception e) {
                    System.out.println("Error in Combiner Record Parsing");
                }
                ;
            }
        }
    }

    public void flushRows() {
        if (rows.length() != 0) {
            try {
                Connection connection = MySQLUtil.getConnection();
                String selectStatement = "INSERT INTO events(ride_id,event_name,created_at,event_body)" +
                        " VALUES " + rows.substring(1, rows.length());
                Statement statement = connection.createStatement();
                statement.execute(selectStatement);
                statement.close();
                connection.close();
                System.out.println("Data Inserted into Repository");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
