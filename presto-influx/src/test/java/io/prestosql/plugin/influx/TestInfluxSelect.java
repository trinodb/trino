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
package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestInfluxSelect
{
    private TestingInfluxServer influxServer;
    private DistributedQueryRunner queryRunner;
    private Session session;

    @BeforeClass
    public void beforeClass()
            throws Exception
    {
        influxServer = new TestingInfluxServer();

        // create a query runner with a real influx connector
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                .setIdentity(Identity.forUser("test")
                        .withRole("catalog", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build()).build();
        queryRunner.installPlugin(new InfluxPlugin());
        queryRunner.createCatalog("catalog", "influx",
                new ImmutableMap.Builder<String, String>()
                        .put("influx.host", influxServer.getHost())
                        .put("influx.port", Integer.toString(influxServer.getPort()))
                        .put("influx.database", TestingInfluxServer.DATABASE)
                        .build());
        session = queryRunner.getDefaultSession();
    }

    @Test
    public void testShow()
    {
        MaterializedResult result = execute("SHOW TABLES");
        assertEquals(result.getOnlyColumn().collect(Collectors.toList()), Collections.singletonList("data"));

        result = execute("DESC data");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("time", "timestamp with time zone", "time", "")
                .row("k1", "varchar", "tag", "")
                .row("k2", "varchar", "tag", "")
                .row("v1", "double", "field", "")
                .row("v2", "double", "field", "")
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public void testSelectAll()
    {
        MaterializedResult result = execute("SELECT * FROM datA");
        assertEquals(result, expect(0, 1, 2));
    }

    @Test
    public void testSelectTagEqual()
    {
        MaterializedResult result = execute("SELECT * FROM data WHERE k1 = 'a'");
        assertEquals(result, expect(0));
    }

    @Test
    public void testSelectTagNotEqual()
    {
        // can't be pushed down
        MaterializedResult result = execute("SELECT * FROM data WHERE k1 != 'd'");
        assertEquals(result, expect(0, 1));
    }

    @Test
    public void testSelectField()
    {
        MaterializedResult result = execute("SELECT v1 FROM data WHERE k1 != 'd'");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, DOUBLE)
                .row(1.)
                .row(3.)
                .build();
        assertEquals(result, expectedColumns);
    }

    @Test
    public void testSelectTagNull()
    {
        MaterializedResult result = execute("SELECT * FROM data WHERE k1 IS NULL");
        assertEquals(result, expect());
    }

    @Test
    public void testSelectFieldNull()
    {
        // can't be pushed down
        MaterializedResult result = execute("SELECT * FROM data WHERE v1 IS NULL");
        assertEquals(result, expect());
    }

    @Test
    public void testSelectFieldEqual()
    {
        MaterializedResult result = execute("SELECT * FROM data WHERE v1 = 1");
        assertEquals(result, expect(0));
    }

    @Test
    public void testSelectFieldBetween()
    {
        MaterializedResult result = execute("SELECT * FROM data WHERE v1 BETWEEN 1 AND 3");
        assertEquals(result, expect(0, 1));
    }

    private synchronized MaterializedResult execute(String sql)
    {
        return queryRunner.getClient().execute(session, sql)
                .getResult()
                .toTestTypes();
    }

    private MaterializedResult expect(int... rows)
    {
        MaterializedResult.Builder expected = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, DOUBLE, DOUBLE);
        for (int row : rows) {
            expected.row(TestingInfluxServer.getColumns(row));
        }
        return expected.build();
    }

    @AfterClass
    public void afterClass()
            throws IOException
    {
        queryRunner.close();
        influxServer.close();
    }
}
