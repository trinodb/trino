package io.trino.plugin.couchbase;

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.couchbase.CouchbaseQueryRunner.TEST_SCHEMA;


public class TestCouchbaseConnector
    extends BaseConnectorTest {

    public static final String CBBUCKET = "trino-test";

    private CouchbaseServer server;

    public TestCouchbaseConnector()
    {
        this.server = new CouchbaseServer(CBBUCKET);
    }
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return CouchbaseQueryRunner.builder(server)
                .addInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
