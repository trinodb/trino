/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.testing.testcontainers;

import org.testcontainers.containers.JdbcDatabaseContainer;

public class SapHanaJdbcContainer
        extends JdbcDatabaseContainer<SapHanaJdbcContainer>
{
    public SapHanaJdbcContainer()
    {
        super(SapHanaDockerInitializer.SAP_HANA_DOCKER_IMAGE);
        withStartupTimeoutSeconds(720);
    }

    @Override
    public String getDriverClassName()
    {
        return "com.sap.db.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl()
    {
        return "jdbc:sap://" + getHost() + ":" + getMappedPort(SapHanaDockerInitializer.SYSTEMDB_PORT);
    }

    @Override
    public String getUsername()
    {
        return "SYSTEM";
    }

    @Override
    public String getPassword()
    {
        return "SapHana1IsCool!";
    }

    @Override
    protected String getTestQueryString()
    {
        return "SELECT 1 FROM DUMMY";
    }
}
