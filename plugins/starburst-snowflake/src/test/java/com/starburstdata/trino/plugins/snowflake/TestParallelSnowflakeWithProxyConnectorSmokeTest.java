/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpRequest;
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testng.services.ManageTestResources;
import org.littleshoot.proxy.ActivityTrackerAdapter;
import org.littleshoot.proxy.FlowContext;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeWithProxyConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private static final Logger log = Logger.get(TestParallelSnowflakeWithProxyConnectorSmokeTest.class);
    private static final String PROXY_USER = "proxyuser";
    private static final String PROXY_PASSWORD = "proxypassword";

    @ManageTestResources.Suppress(because = "Mock to remote server")
    private final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    private final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    private final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    @SuppressWarnings("UnusedVariable")
    @ManageTestResources.Suppress(because = "Mock to remote database")
    private final Closeable proxy = closer.register(createProxyServer());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return parallelBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(Map.of(
                        "snowflake.proxy.enabled", "true",
                        "snowflake.proxy.host", "localhost",
                        "snowflake.proxy.port", "8888",
                        "snowflake.proxy.protocol", "http",
                        "snowflake.proxy.username", PROXY_USER,
                        "snowflake.proxy.password", PROXY_PASSWORD))
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_ARRAY:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
            case SUPPORTS_JOIN_PUSHDOWN:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    private Closeable createProxyServer()
    {
        return new CloseableProxyServer(DefaultHttpProxyServer.bootstrap()
                .withPort(8888)
                .withTransparent(true)
                .plusActivityTracker(new ActivityTrackerAdapter()
                {
                    @Override
                    public void requestReceivedFromClient(FlowContext flowContext, HttpRequest httpRequest)
                    {
                        log.info("Proxying request to " + httpRequest.uri());
                    }
                })
                .withProxyAuthenticator(new ProxyAuthenticator()
                {
                    @Override
                    public boolean authenticate(String userName, String password)
                    {
                        return userName.contentEquals(PROXY_USER) && password.contentEquals(PROXY_PASSWORD);
                    }

                    @Override
                    public String getRealm()
                    {
                        return null;
                    }
                })
                .start());
    }

    @SuppressWarnings("UnusedVariable")
    private record CloseableProxyServer(HttpProxyServer proxy)
            implements Closeable
    {
        @Override
        public void close()
        {
            proxy.stop();
        }
    }
}
