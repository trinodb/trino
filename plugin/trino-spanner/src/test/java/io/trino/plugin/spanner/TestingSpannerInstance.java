package io.trino.plugin.spanner;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TestingSpannerInstance
        implements AutoCloseable
{
    private final String SPANNER_IMAGE = "gcr.io/cloud-spanner-emulator/emulator:latest";

    private final SpannerEmulatorContainer emulatorContainer;
    private final String PROJECT = "test-project";
    private final String INSTANCE = "test-instance";
    private final String DATABASE = "trinodb";
    private Database database;
    private SpannerOptions options;
    private Spanner spanner;
    private DatabaseId databaseId;
    private InstanceId instanceId;
    private String host = null;

    public TestingSpannerInstance()
            throws ExecutionException, InterruptedException
    {
        this.emulatorContainer = new SpannerEmulatorContainer(DockerImageName.parse(SPANNER_IMAGE))
                .withExposedPorts(9010, 9020);
        emulatorContainer.start();
        //this.host = "0.0.0.0:" + emulatorContainer.getEmulatorGrpcEndpoint().split(":")[1];
        this.host=emulatorContainer.getEmulatorGrpcEndpoint();
        options = SpannerOptions
                .newBuilder()
                .setEmulatorHost(emulatorContainer.getEmulatorGrpcEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(PROJECT)
                .build();
        this.spanner = options.getService();
        this.instanceId = createInstance();
        this.database = createDatabase();

        this.databaseId = DatabaseId.of(instanceId, DATABASE);
    }

    private static void execute(String url, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, new Properties());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
            throws ExecutionException, InterruptedException
    {
        TestingSpannerInstance spannerInstance = new TestingSpannerInstance();
        Thread.sleep(Duration.ofHours(1).toMillis());
    }

    public String getDatabaseId()
    {
        return DATABASE;
    }

    public String getInstanceId()
    {
        return INSTANCE;
    }

    public String getProjectId()
    {
        return PROJECT;
    }

    public String getHost()
    {
        return host;
    }

    private Database createDatabase()
            throws InterruptedException, ExecutionException
    {
        DatabaseAdminClient dbAdminClient = options.getService().getDatabaseAdminClient();
        return dbAdminClient
                .createDatabase(
                        INSTANCE,
                        DATABASE, new ArrayList<>())
                .get();
    }

    private InstanceId createInstance()
            throws InterruptedException, ExecutionException
    {
        InstanceConfigId instanceConfig = InstanceConfigId.of(PROJECT, "emulator-config");
        InstanceId instanceId = InstanceId.of(PROJECT, INSTANCE);
        InstanceAdminClient insAdminClient = spanner.getInstanceAdminClient();
        return insAdminClient
                .createInstance(
                        InstanceInfo
                                .newBuilder(instanceId)
                                .setNodeCount(1)
                                .setDisplayName("Test instance")
                                .setInstanceConfigId(instanceConfig)
                                .build())
                .get().getId();
    }

    public void execute(@Language("SQL") String sql)
    {
        execute(getJdbcUrl(), sql);
    }

    public String getJdbcUrl()
    {
        return String.format("jdbc:cloudspanner://%s/projects/%s/instances/%s/databases/%s;autoConfigEmulator=true",
                emulatorContainer.getEmulatorGrpcEndpoint(), PROJECT, INSTANCE, DATABASE);
    }

    @Override
    public void close()
            throws Exception
    {
        emulatorContainer.stop();
    }
}
