package perfjdbc;

import java.util.List;

public class ConfigDatabase {
    public String name;
    public String db_type;
    public String db_name;
    public String host;
    public String jdbc_driver_class;
    public String jdbc_url;
    public String port;
    public String user;
    public String password;
    public String bottom_limit;
    public String top_limit;
    public String measurement_duration;
    public List<String> connection_types;

    @Override
    public String toString() {
        return "ConfigDatabase{" +
                "name='" + name + '\'' +
                ", db_type='" + db_type + '\'' +
                ", database='" + db_name + '\'' +
                ", host='" + host + '\'' +
                ", jdbc_driver_class='" + jdbc_driver_class + '\'' +
                ", jdbc_url='" + jdbc_url + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", top_limit='" + top_limit + '\'' +
                ", measure_duration='" + measurement_duration + '\'' +
                '}';
    }
}
