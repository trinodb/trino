/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import java.sql.SQLException;

import static com.starburstdata.trino.plugins.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;

public class OracleTestUsers
{
    public static final String USER = "presto_test_user";
    public static final String PASSWORD = "testsecret";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";

    private OracleTestUsers() {}

    public static void createStandardUsers()
    {
        createUser(ALICE_USER);
        createUser(BOB_USER);
        createUser(CHARLIE_USER);

        executeInOracle(
                "CREATE OR REPLACE VIEW user_context AS " +
                        "SELECT " +
                        "sys_context('USERENV', 'SESSION_USER') AS session_user_column," +
                        "sys_context('USERENV', 'SESSION_SCHEMA') AS session_schema_column," +
                        "sys_context('USERENV', 'CURRENT_SCHEMA') AS current_schema_column," +
                        "sys_context('USERENV', 'PROXY_USER') AS proxy_user_column " +
                        "FROM dual");

        executeInOracle(format("GRANT SELECT ON user_context to %s", ALICE_USER));
        executeInOracle(format("GRANT SELECT ON user_context to %s", BOB_USER));
    }

    public static void createUser(String user)
    {
        try {
            executeInOracle(format("CREATE USER %s IDENTIFIED BY \"vier1Str0ngP@55vvord\"", user));
        }
        catch (RuntimeException e) {
            propagateUnlessUserAlreadyExists(e);
        }
        executeInOracle(format("ALTER USER %s GRANT CONNECT THROUGH %s", user, USER));
        executeInOracle(format("ALTER USER %s QUOTA UNLIMITED ON USERS", user));
        executeInOracle(format("GRANT CREATE SESSION TO %s", user));
    }

    private static void propagateUnlessUserAlreadyExists(RuntimeException e)
    {
        if (e.getCause() instanceof SQLException && ((SQLException) e.getCause()).getErrorCode() == 1920) {
            // ORA-01920: user name '<user>' conflicts with another user or role name
        }
        else {
            throw e;
        }
    }
}
