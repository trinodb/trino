/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import java.sql.SQLException;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;

public class OracleTestUsers
{
    public static final String USER = "presto_test_user";
    public static final String PASSWORD = "testsecret";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";
    public static final String UNKNOWN_USER = "non_existing_user";
    public static final String KERBERIZED_USER = "test";

    private OracleTestUsers() {}

    public static void createStandardUsers()
    {
        createKerberizedUser(KERBERIZED_USER);
        createUser(ALICE_USER, KERBERIZED_USER);
        createUser(BOB_USER, KERBERIZED_USER);
        createUser(CHARLIE_USER, KERBERIZED_USER);

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

    public static void createKerberizedUser(String user)
    {
        try {
            executeInOracle(format("CREATE USER %s IDENTIFIED EXTERNALLY AS '%s@TESTING-KRB.STARBURSTDATA.COM'", user, user));
        }
        catch (RuntimeException e) {
            propagateUnlessUserAlreadyExists(e);
        }
        executeInOracle("GRANT CONNECT,RESOURCE TO " + user);
        executeInOracle("GRANT UNLIMITED TABLESPACE TO " + user);
        executeInOracle("GRANT CREATE ANY TABLE TO " + user);
        executeInOracle("GRANT DROP ANY TABLE TO " + user);
        executeInOracle("GRANT INSERT ANY TABLE TO " + user);
        executeInOracle("GRANT SELECT ANY TABLE TO " + user);
        executeInOracle("GRANT ALTER ANY TABLE TO " + user);
        executeInOracle("GRANT LOCK ANY TABLE TO " + user);
        executeInOracle("GRANT ANALYZE ANY TO " + user);
    }

    public static void createUser(String user, String kerberizedUser)
    {
        try {
            executeInOracle(format("CREATE USER %s IDENTIFIED BY \"vier1Str0ngP@55vvord\"", user));
        }
        catch (RuntimeException e) {
            propagateUnlessUserAlreadyExists(e);
        }
        executeInOracle(format("ALTER USER %s GRANT CONNECT THROUGH %s", user, USER));
        executeInOracle(format("ALTER USER %s GRANT CONNECT THROUGH %s", user, kerberizedUser));
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
