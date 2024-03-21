package perfjdbc;

public class ConfigBase {
    public String query;
    public Integer measurement_iterations;

    @Override
    public String toString() {
        return "BasicConfig{" +
                "query='" + query + '\'' +
                '}';
    }
}
