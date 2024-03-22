package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestSpannerPlugin
{
    @Test
    public void testCreateConnector()
            throws Exception
    {
        Plugin plugin = new SpannerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        TestingSpannerInstance instance = new TestingSpannerInstance();
        factory.create("test", ImmutableMap.of(
                        "spanner.credentials.file", "credentials.json",
                        "spanner.instanceId", instance.getInstanceId()
                        , "spanner.projectId", instance.getProjectId()
                        , "spanner.database", instance.getDatabaseId()
                        , "spanner.emulated", "true"
                        , "spanner.emulated.host", instance.getHost()
                ),
                new TestingConnectorContext()).shutdown();
        instance.close();
    }
}
