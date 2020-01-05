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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
        TestData.initServer(influxServer);

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
                .row("tag1", "varchar", "tag", "")
                .row("tag2", "varchar", "tag", "")
                .row("field1", "double", "field", "")
                .row("field2", "double", "field", "")
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
        MaterializedResult result = execute("SELECT * FROM data WHERE tAG1 = 'a'");
        assertEquals(result, expect(0));
    }

    @Test
    public void testSelectTagNotEqual()
    {
        // can't be pushed down
        MaterializedResult result = execute("SELECT * FROM data WHERE tAG1 != 'd'");
        assertEquals(result, expect(0, 1));
    }

    @Test
    public void testSelectField()
    {
        MaterializedResult result = execute("SELECT fIELD1 FROM data WHERE tAG1 != 'd'");
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
        MaterializedResult result = execute("SELECT * FROM data WHERE tAG1 IS NULL");
        assertEquals(result, expect());
    }

    @Test
    public void testSelectFieldNull()
    {
        // can't be pushed down
        MaterializedResult result = execute("SELECT * FROM data WHERE fIELD1 IS NULL");
        assertEquals(result, expect());
    }

    @Test
    public void testSelectFieldEqual()
    {
        MaterializedResult result = execute("SELECT * FROM data WHERE fIELD1 = 1");
        assertEquals(result, expect(0));
    }

    @Test
    public void testSelectFieldBetween()
    {
        MaterializedResult result = execute("SELECT * FROM data WHERE fIELD1 BETWEEN 1 AND 3");
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
            expected.row(TestData.getColumns(row));
        }
        return expected.build();
    }

    @AfterClass
    public void afterClass()
    {
        queryRunner.close();
        influxServer.close();
    }

    private static class TestData
    {
        private static final Instant[] T = new Instant[] {
                Instant.parse("2019-12-10T21:00:04.446Z"),
                Instant.parse("2019-12-10T21:00:20.446Z"),
                Instant.parse("2019-12-10T22:00:04.446Z"),
        };
        private static final String[] TAG1 = new String[] {"a", "b", "d"};
        private static final String[] TAG2 = new String[] {"b", "c", "b"};
        private static final double[] FIELD1 = new double[] {1, 3, 5};
        private static final double[] FIELD2 = new double[] {2, 4, 6};

        private static void initServer(TestingInfluxServer server)
        {
            for (int row = 0; row < 3; row++) {
                String line = String.format("%s,tag1=%s,tag2=%s field1=%f,field2=%f %d000000", TestingInfluxServer.MEASUREMENT, TAG1[row], TAG2[row], FIELD1[row], FIELD2[row], T[row].toEpochMilli());
                server.getInfluxClient().write(TestingInfluxServer.RETENTION_POLICY, line);
            }
        }

        private static Object[] getColumns(int row)
        {
            return new Object[] {ZonedDateTime.ofInstant(T[row], ZoneId.of("UTC")), TAG1[row], TAG2[row], FIELD1[row], FIELD2[row]};
        }
    }
}
