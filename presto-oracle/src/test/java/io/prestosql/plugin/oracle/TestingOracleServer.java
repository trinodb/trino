/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.oracle;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.OracleContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingOracleServer
        extends OracleContainer
        implements Closeable
{
    public static final String TEST_SCHEMA = "tpch";
    public static final String TEST_USER = "tpch";
    public static final String TEST_PASS = "oracle";

    public TestingOracleServer()
    {
        super("wnameless/oracle-xe-11g-r2");

        // this is added to allow more processes on database, otherwise the tests end up giving a ORA-12519
        // to fix this we have to change the number of processes of SPFILE
        // but this command needs a database restart and if we restart the docker the configuration is lost.
        // configuration added:
        // ALTER SYSTEM SET processes=500 SCOPE=SPFILE
        // ALTER SYSTEM SET disk_asynch_io = FALSE SCOPE = SPFILE
        this.addFileSystemBind("src/test/resources/spfileXE.ora",
                "/u01/app/oracle/product/11.2.0/xe/dbs/spfileXE.ora",
                BindMode.READ_ONLY);

        start();
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), super.getUsername(), super.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(format("CREATE TABLESPACE %s DATAFILE 'test_db.dat' SIZE 100M ONLINE", TEST_SCHEMA));
            statement.execute(format("CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE %s", TEST_USER, TEST_PASS, TEST_SCHEMA));
            statement.execute(format("GRANT UNLIMITED TABLESPACE TO %s", TEST_USER));
            statement.execute(format("GRANT ALL PRIVILEGES TO %s", TEST_USER));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql)
    {
        execute(sql, TEST_USER, TEST_PASS);
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        stop();
    }
}
