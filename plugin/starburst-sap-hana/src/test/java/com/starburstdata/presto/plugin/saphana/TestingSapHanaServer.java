/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.starburstdata.presto.testing.testcontainers.SapHanaDockerInitializer;
import com.starburstdata.presto.testing.testcontainers.SapHanaJdbcContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class TestingSapHanaServer
        implements Closeable
{
    private static final SapHanaDockerInitializer dockerInitializer = new SapHanaDockerInitializer((genericContainer, port) -> genericContainer.addExposedPort(port));

    private final SapHanaJdbcContainer dockerContainer;

    public TestingSapHanaServer()
    {
        dockerContainer = new SapHanaJdbcContainer();
        dockerInitializer.apply(dockerContainer);
        dockerContainer.start();
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }

    public void execute(String sql)
    {
        try (Connection connection = dockerContainer.createConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException | RuntimeException e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        return dockerContainer.getJdbcUrl();
    }

    public String getUser()
    {
        return dockerContainer.getUsername();
    }

    public String getPassword()
    {
        return dockerContainer.getPassword();
    }
}
