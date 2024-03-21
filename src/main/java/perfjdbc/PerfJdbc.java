package perfjdbc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PerfJdbc {
    // 128 MB
    private final static long ALLOCATOR_LIMIT = 1024 * 1024 * 128;
    private final static List<String> ROW_LIMITS = List.of("1000", "10000", "100000", "1000000", "5000000");

    public enum ConnectionType {
        JDBC, JDBC_ARROW, ADBC
    }

    public enum DbType {
        POSTGRESQL, SNOWFLAKE, VERTICA
    }

    @State(Scope.Benchmark)
    public static class Params {
        @Param({"query.sql"})
        String sqlFileNameIn;
        // For the future usage, if we need to distinguish between different databases in the test
        @SuppressWarnings("unused")
        @Param("POSTGRESQL")
        DbType dbType;
        @Param({"org.postgresql.Driver"})
        String driver;
        @Param({"jdbc:postgresql://localhost:5432/tiger"})
        String url;
        @Param({"tiger"})
        String user;
        @Param({"passw0rd"})
        String password;
        @Param({"1000", "10000", "100000", "1000000", "10000000"})
        String limit;
        @Param({"JDBC", "JDBC_ARROW", "ADBC"})
        ConnectionType connectionType;

        // setup
        String query;
        Connection c;
        BufferAllocator bufferAllocator;

        // adbc setup
        AdbcConnection ac;
        AdbcDatabase adb;
        String dbPassword;

        @Setup
        public void setUp() throws ClassNotFoundException, SQLException, IOException, AdbcException, IllegalArgumentException {
            final Path sqlFileName = Path.of(sqlFileNameIn);
            query = Files.readString(sqlFileName) + " LIMIT " + limit;

            Dotenv dotenv;
            try {
                dotenv = Dotenv.load();
            } catch (Exception e) {
                throw new IOException("No .env file found");
            }
            dbPassword = dotenv.get(password);
            if (dbPassword == null) {
                throw new IllegalArgumentException(String.format("Password '%s' not found in .env file", password));
            }

            if (ConnectionType.JDBC_ARROW == connectionType || ConnectionType.ADBC == connectionType) {
                bufferAllocator = new RootAllocator(ALLOCATOR_LIMIT);
            }

            if (ConnectionType.JDBC == connectionType || ConnectionType.JDBC_ARROW == connectionType) {
                Class.forName(driver);
                c = DriverManager.getConnection(url, user, dbPassword);
            }
            if (ConnectionType.ADBC == connectionType) {
                final Map<String, Object> parameters = new HashMap<>();
                parameters.put(AdbcDriver.PARAM_URI.getKey(), url);
                parameters.put(AdbcDriver.PARAM_USERNAME.getKey(), user);
                parameters.put(AdbcDriver.PARAM_PASSWORD.getKey(), dbPassword);
                adb = new JdbcDriver(bufferAllocator).open(parameters);
                ac = adb.connect();
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            if (c != null) {
                c.close();
            }
            if (ac != null) {
                ac.close();
            }
            if (adb != null) {
                adb.close();
            }
            if (bufferAllocator != null) {
                bufferAllocator.close();
            }
        }
    }

    @Benchmark
    public void execQuery(Blackhole bh, Params params) throws Exception {
        ResultSet rs = null;
        Statement stmt = null;
        AdbcStatement astmt = null;
        try {
            switch (params.connectionType) {
                case ADBC:
                    astmt = params.ac.createStatement();
                    astmt.setSqlQuery(params.query);
                    try (AdbcStatement.QueryResult queryResult = astmt.executeQuery();
                         final ArrowReader reader = queryResult.getReader()) {
                        while (reader.loadNextBatch()) {
                            bh.consume(reader.getDictionaryVectors());
                        }
                    }
                    break;
                case JDBC:
                    stmt = params.c.createStatement();
                    //noinspection SqlSourceToSinkFlow
                    rs = stmt.executeQuery(params.query);
                    while (rs.next()) {
                        // We rely on that the SQL query returns exactly 3 columns
                        bh.consume(rs.getString(1));
                        bh.consume(rs.getString(2));
                        bh.consume(rs.getString(3));
                    }
                    break;
                case JDBC_ARROW:
                    stmt = params.c.createStatement();
                    //noinspection SqlSourceToSinkFlow
                    rs = stmt.executeQuery(params.query);
                    try (ArrowVectorIterator it = JdbcToArrow.sqlToArrowVectorIterator(rs, params.bufferAllocator)) {
                        while (it.hasNext()) {
                            try (VectorSchemaRoot root = it.next()) {
                                bh.consume(root);
                            }
                        }
                    }
                    break;
            }
        } catch (Exception e) {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (astmt != null) {
                astmt.close();
            }
            throw e;
        }
    }

    private static ChainedOptionsBuilder getChainedOptionsBuilder(String locationName, ConfigDatabase database,  ConfigPerf configPerf) {
        ChainedOptionsBuilder opt = new OptionsBuilder()
                .include(PerfJdbc.class.getSimpleName())
                .result(String.format("results/java_results_%s_%s.json", locationName, database.name))
                .resultFormat(ResultFormatType.JSON)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .forks(1)
                .warmupTime(TimeValue.seconds(10))
                .warmupIterations(1)
                .measurementIterations(configPerf.config.measurement_iterations)
                .measurementTime(TimeValue.seconds(Long.parseLong(database.measurement_duration)));
        opt = opt.param("sqlFileNameIn", configPerf.config.query);
        opt = opt.param("dbType", database.db_type);
        opt = opt.param("driver", database.jdbc_driver_class);
        opt = opt.param("url", database.jdbc_url);
        opt = opt.param("user", database.user);

        opt = opt.param("password", database.password);
        if (database.connection_types != null && !database.connection_types.isEmpty()) {
            opt = opt.param("connectionType", database.connection_types.toArray(new String[0]));
        } else {
            // Default is all connection types
            opt = opt.param("connectionType", List.of("JDBC", "JDBC_ARROW", "ADBC").toArray(new String[0]));
        }
        String bottomLimit = database.bottom_limit != null ? database.bottom_limit : ROW_LIMITS.get(0);
        String topLimit = database.top_limit != null ? database.top_limit : ROW_LIMITS.get(ROW_LIMITS.size() - 1);
        List<String> limits = ROW_LIMITS.subList(ROW_LIMITS.indexOf(bottomLimit), ROW_LIMITS.indexOf(topLimit) + 1);
        opt = opt.param("limit", limits.toArray(new String[0]));
        return opt;
    }

    public static void main(String[] args) throws Throwable {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.findAndRegisterModules();
        ConfigPerf configPerf = mapper.readValue(new File("config.yaml"), ConfigPerf.class);
        for (ConfigLocation location : configPerf.locations) {
            for (ConfigDatabase database : location.databases) {
                System.out.println("-".repeat(120));
                System.out.printf("Run test: location=%s database=%s%n", location.name, database.name);
                System.out.println("-".repeat(120));
                ChainedOptionsBuilder opt = getChainedOptionsBuilder(location.name, database, configPerf);

                new Runner(opt.build()).run();
            }
        }
    }

}
