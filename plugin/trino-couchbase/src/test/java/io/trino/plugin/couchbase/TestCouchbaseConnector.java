package io.trino.plugin.couchbase;

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.assertions.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;


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

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_NEGATIVE_DATE, // min date is 0001-01-01
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_UPDATE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_MERGE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

//    @Test
//    @Override
    public void testTopNPushdownLimited() {
        // TopN over limit with filter
        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM orders WHERE orderdate = DATE '1995-09-16' LIMIT 20) " +
                "ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();
    }
}
