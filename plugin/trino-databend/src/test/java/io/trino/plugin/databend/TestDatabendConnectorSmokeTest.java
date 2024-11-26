package io.trino.plugin.databend;

import io.trino.testing.QueryRunner;

public class TestDatabendConnectorSmokeTest
        extends BaseDatabendConnectorSmokeTest
{
    protected TestingDatabendServer databendServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer());
        return DatabendQueryRunner.builder(databendServer)
                .addConnectorProperty("databend.connection_timeout", "60")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
