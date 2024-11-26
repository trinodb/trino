package io.trino.plugin.databend;


import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDatabendConnectorTest
        extends BaseDatabendConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer(TestingDatabendServer.DATABEND_DEFAULT_IMAGE));
        return DatabendQueryRunner.builder(databendServer)
                .addConnectorProperty("databend.connection-timeout", "60")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
