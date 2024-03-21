package perfjdbc;

import java.util.List;

public class ConfigPerf {
    public ConfigBase config;
    public List<ConfigLocation> locations;

    @Override
    public String toString() {
        return "PerfConfig{" +
                "config=" + config +
                ", locations=" + locations +
                '}';
    }
}
