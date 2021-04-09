import org.apache.commons.cli.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.exit;


class ConnectMSSQLServer  {

    private static final String JDBCString = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static Connection dbConn = null;
    private static String dbConnStr = "";
    private static String username = "";
    private static String password = "";

    public ConnectMSSQLServer (String host,
                               String port,
                               String username,
                               String password,
                               String db) {
        this.dbConnStr = connectionString(host, port, db);
        System.out.println("DB connection string : " + this.dbConnStr);
        this.username = username;
        this.password = password;
    }

    public static String connectionString(String host,
                                          String port,
                                          String db) {
        String connStr = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;integratedSecurity=true;", host, port, db);
        return connStr;
    }

    public Connection getDbConn(String db_connect_string,
                                String db_userid,
                                String db_password) {
        try {
            Class.forName(JDBCString);
            this.dbConn = DriverManager.getConnection(db_connect_string,
                    db_userid, db_password);
            System.out.println("connected");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Cannot connect to database, check your credentials");
            exit(1);
        }
        return dbConn;
    }


    public Connection connectDb() {
            if (this.dbConn != null)  {
                System.out.println("Connected already!");
                return this.dbConn;
            }
            else {
                // get db, user, pass from settings file
                return getDbConn(this.dbConnStr, this.username, this.password);
            }
    }

    public void getQuery(String queryString) {
        System.out.println(String.format("Preparing statement : %s", queryString));
        try ( Connection connection = connectDb();
              PreparedStatement statement = connection.prepareStatement(queryString);
                ) {
            System.out.println(String.format("Executing statement : %s", queryString));
            try (ResultSet resultSet = statement.executeQuery()) {
                // collect column names
                List<String> columnNames = new ArrayList<>();
                ResultSetMetaData rsmd = resultSet.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    columnNames.add(rsmd.getColumnLabel(i));
                }

                int rowIndex = 0;
                while (resultSet.next()) {
                    rowIndex++;
                    // collect row data as objects in a List
                    List<Object> rowData = new ArrayList<>();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        rowData.add(resultSet.getObject(i));
                    }
                    // for test purposes, dump contents to check our results
                    // (the real code would pass the "rowData" List to some other routine)
                    System.out.printf("Row %d%n", rowIndex);
                    for (int colIndex = 0; colIndex < rsmd.getColumnCount(); colIndex++) {
                        String objType = "null";
                        String objString = "";
                        Object columnObject = rowData.get(colIndex);
                        if (columnObject != null) {
                            objString = columnObject.toString() + " ";
                            objType = columnObject.getClass().getName();
                        }
                        System.out.printf("  %s: %s(%s)%n",
                                columnNames.get(colIndex), objString, objType);
                    }
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }

}

public class TestSQL {

    public static HashMap<String, String> readProperties(String PropertyFilePath) {

        HashMap<String, String> properties = new HashMap<String, String>();
        Properties prop = new Properties();
        System.out.println("Reading properties file!");

        try {
            prop.load(new FileInputStream(PropertyFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        properties.put("host", prop.getProperty("HOST"));
        properties.put("port", prop.getProperty("PORT"));
        properties.put("db", prop.getProperty("DB"));
        properties.put("username", prop.getProperty("USER"));
        properties.put("password", prop.getProperty("PASS"));
        return properties;

    }

    public static List<String> readSQLQueries(String queryFilePath) {
        List<String> queries = null;
        System.out.println("Reading SQL Queries file!");
        try (Stream<String> lines = Files.lines(Paths.get(queryFilePath))) {
            queries = lines.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("Failed to load file.");
            System.out.println(e.getMessage());
        }
        return queries;
    }


    public static void main(String[] args)  {

        Options options = new Options();

        Option query = new Option("q", "query", true, "DB Query input file path");
        query.setRequired(true);
        options.addOption(query);

        Option properties = new Option("p", "properties", true, "Properties file path");
        properties.setRequired(true);
        options.addOption(properties);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            exit(1);
        }

        String propertiesFilePath = cmd.getOptionValue("properties");
        String queryFilePath = cmd.getOptionValue("query");

        System.out.println(String.format("Properties file path : {}: " + propertiesFilePath));
        System.out.println(String.format("Query file path : {} : "+ queryFilePath));

        HashMap<String, String> dBConnVal = readProperties(propertiesFilePath);
        List<String> sqlQueries = readSQLQueries(queryFilePath);
        for(String key : dBConnVal.keySet()) {
            String value = "";
            value = dBConnVal.get(key);
            System.out.println(key + " has a value of " + value + " on positions: ");
        }
        System.out.println("connecting to the database");

        ConnectMSSQLServer connection = new ConnectMSSQLServer(dBConnVal.get("host"), dBConnVal.get("port"), dBConnVal.get("db"), dBConnVal.get("username"), dBConnVal.get("password"));
        try {
            connection.connectDb();
        }
        catch (Exception e)  {
            System.out.println("unable to connect to Database");
            exit(1);
        }

        System.out.println(" Now executing SQL Queries");
        for(String q : sqlQueries) {
            System.out.println(q);
            connection.getQuery(q);
        }

    }
}
