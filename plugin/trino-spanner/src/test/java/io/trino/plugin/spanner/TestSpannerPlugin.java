package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.spanner.SpannerPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestSpannerPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new SpannerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:cloudspanner://0.0.0.0:9010/projects/spanner-project/instances/spanner-instance/databases/spanner-database;autoConfigEmulator=true"), new TestingConnectorContext()).shutdown();
    }
}
