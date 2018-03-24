package snapp.etl.combiner;


import snapp.etl.db.MySQLUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

public class RideCombinerEngine extends BaseCombinerEngine {
    private HashMap<String, event> eventMap = new HashMap<>();

    private String getDeleteQueryString() {
        String retentionTime = configuration.getConfig("ride.retention.policy");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm");
        String query = String.format("DELETE FROM events WHERE TIMESTAMPDIFF(MINUTE,created_at,'%s') > %s", simpleDateFormat.format(new Date()), retentionTime);
        return query;
    }

    private String getRideIDQueryString() {
        return "select \n" +
                "id,\n" +
                "ride_id , \n" +
                "event_name , \n" +
                "created_at,\n" +
                "event_body,\n" +
                "count(*) over(partition by ride_id) as ride_id_count\n" +
                "from events where ride_id in (\n" +
                "select ride_id from events group by ride_id having count(*)>4\n" +
                ")   \n" +
                "order by ride_id , created_at \n" +
                "\n";
    }

    @Override
    void engine() {
        try {
            Connection connection = MySQLUtil.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(getDeleteQueryString());
            ResultSet resultSet = statement.executeQuery(getRideIDQueryString());
            while (resultSet.next()) {
                int ride_id_count = resultSet.getInt("ride_id_count");
                for (int i = 1; i <= ride_id_count; i++) {
                    //Operation room
                    int ride_id = resultSet.getInt("ride_id");
                    int id = resultSet.getInt("id");
                    String event_name = resultSet.getString("event_name");
                    String event_body = resultSet.getString("event_body");
                    String created_at = resultSet.getString("created_at");
                    eventMap.put(event_name, new event(ride_id, id, event_name, event_body, created_at));
// end of Operation room
                    if (ride_id_count - i > 0) {
                        resultSet.next();
                    }
                }
                //Action Room
                System.out.println("--------------------------------");
                System.out.println(eventMap.size());
                Set<String> keySet = eventMap.keySet();
                for (String s : keySet) {
                    System.out.println(s);
                }
                eventMap.clear();
//end of Action Room
            }
            //   statement.execute(getDeleteQueryString());


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    long setInterval() {
        return Long.parseLong(String.valueOf(configuration.getConfig("ride.combiner.interval")));
    }

    @Override
    void failureStatements() {

    }

    private class event {
        int ride_id;
        int id;
        String event_name;
        String event_body;
        String created_at;

        public event(int ride_id, int id, String event_name, String event_body, String created_at) {
            this.ride_id = ride_id;
            this.id = id;
            this.event_name = event_name;
            this.event_body = event_body;
            this.created_at = created_at;
        }

        public int getRide_id() {
            return ride_id;
        }

        public void setRide_id(int ride_id) {
            this.ride_id = ride_id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getEvent_name() {
            return event_name;
        }

        public void setEvent_name(String event_name) {
            this.event_name = event_name;
        }

        public String getEvent_body() {
            return event_body;
        }

        public void setEvent_body(String event_body) {
            this.event_body = event_body;
        }

        public String getCreated_at() {
            return created_at;
        }

        public void setCreated_at(String created_at) {
            this.created_at = created_at;
        }
    }
}


