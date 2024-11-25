package io.trino.plugin.databend;

import io.trino.testing.ResourcePresence;
import org.testcontainers.databend.DatabendContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingDatabendServer
        implements Closeable
{
    private static final DockerImageName DATABEND_IMAGE = DockerImageName.parse("datafuselabs/databend:v1.2.615");
    public static final DockerImageName DATABEND_LATEST_IMAGE = DATABEND_IMAGE.withTag("v1.2.615");
    public static final DockerImageName DATABEND_DEFAULT_IMAGE = DATABEND_IMAGE.withTag("v1.2.615");

    private final DatabendContainer dockerContainer;

    public TestingDatabendServer()
    {
        this(DATABEND_DEFAULT_IMAGE);
    }

    public TestingDatabendServer(DockerImageName image)
    {
        dockerContainer = new DatabendContainer(image);

        dockerContainer.start();
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        return format("jdbc:databend://%s:%s/", dockerContainer.getHost(),
                dockerContainer.getMappedPort(8000));
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainer.getContainerId() != null;
    }
}
