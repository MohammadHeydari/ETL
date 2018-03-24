package snapp.etl.db;


import snapp.etl.util.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLUtil {
    private MySQLUtil() {
    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Configuration configuration = Configuration.getInstance();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager
                .getConnection(String.format("jdbc:mysql://%s/%s?"
                                + "user=%s&password=%s"
                        , configuration.getConfig("mysql.db.address"),
                        configuration.getConfig("mysql.db.name"),
                        configuration.getConfig("mysql.username"),
                        configuration.getConfig("mysql.password")));

        return connection;
    }

}
