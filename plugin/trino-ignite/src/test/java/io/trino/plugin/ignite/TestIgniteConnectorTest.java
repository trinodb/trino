package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;

public class TestIgniteConnectorTest
    extends BaseJdbcConnectorTest
{
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = closeAfterClass(new TestingIgniteServer());
        return createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                REQUIRED_TPCH_TABLES);
    }
}
