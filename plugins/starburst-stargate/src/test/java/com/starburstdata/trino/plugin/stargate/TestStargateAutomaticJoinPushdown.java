/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.starburstdata.presto.plugin.jdbc.joinpushdown.BaseAutomaticJoinPushdownTest;
import io.trino.Session;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createStargateQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static java.lang.String.format;

public class TestStargateAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    private TestingPostgreSqlServer postgreSqlServer;
    private DistributedQueryRunner remoteStarburst;
    private Session remoteSession;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());

        remoteStarburst = closeAfterClass(StargateQueryRunner.createRemoteStarburstQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(
                        "connection-url", postgreSqlServer.getJdbcUrl(),
                        "connection-user", postgreSqlServer.getUser(),
                        "connection-password", postgreSqlServer.getPassword()),
                List.of(),
                Optional.empty()));

        remoteSession = Session.builder(remoteStarburst.getDefaultSession())
                .setCatalog("postgresql")
                .setSchema("tiny")
                .build();

        return createStargateQueryRunner(
                false,
                Map.of("connection-url", stargateConnectionUrl(remoteStarburst, "postgresql")));
    }

    @Override
    public void testJoinPushdownWithEmptyStatsInitially()
    {
        throw new SkipException("Remote with PostgreSQL statistics are automatically collected");
    }

    @Override
    protected void gatherStats(String tableName)
    {
        onRemoteDatabase(handle -> {
            handle.execute("ANALYZE " + tableName);
            for (int i = 0; i < 5; i++) {
                long actualCount = handle.createQuery("SELECT count(*) FROM " + tableName).mapTo(Long.class).one();

                long estimatedCount = handle.createQuery(format("SELECT reltuples FROM pg_class WHERE oid = '%s'::regclass::oid", tableName))
                        .mapTo(Long.class)
                        .one();
                if (actualCount == estimatedCount) {
                    return;
                }
                handle.execute("ANALYZE " + tableName);
            }

            throw new IllegalStateException("Stats not gathered"); // for small test tables reltuples should be exact
        });
    }

    private <E extends Exception> void onRemoteDatabase(HandleConsumer<E> callback)
            throws E
    {
        Properties properties = new Properties();
        properties.setProperty("currentSchema", "tiny");
        properties.setProperty("user", postgreSqlServer.getUser());
        properties.setProperty("password", postgreSqlServer.getPassword());
        Jdbi.create(postgreSqlServer.getJdbcUrl(), properties)
                .useHandle(callback);
    }

    @Override
    protected SqlExecutor tableCreator()
    {
        return sql -> remoteStarburst.execute(remoteSession, sql);
    }
}
