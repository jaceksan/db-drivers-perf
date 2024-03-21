package perfjdbc;

import java.util.List;

public class ConfigLocation {
    public String name;
    public List<ConfigDatabase> databases;

    @Override
    public String toString() {
        return "ConfigLocation{" +
                "name='" + name + '\'' +
                ", databases=" + databases +
                '}';
    }
}
