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

import io.prestosql.Session;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestInfluxSelect
        implements Closeable
{
    private static final DateTimeFormatter SQL_TIMESTAMP_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS 'UTC'")
            .withZone(ZoneOffset.UTC);
    private InfluxQueryRunner queryRunner;
    private Session session;

    @BeforeClass
    public void beforeClass()
            throws Exception
    {
        queryRunner = new InfluxQueryRunner();
        session = queryRunner.createSession("schema");
        TestData.init(queryRunner);
    }

    @Test
    public void testShow()
    {
        MaterializedResult result = execute("SHOW TABLES");
        assertEquals(result.getOnlyColumn().collect(Collectors.toList()), Collections.singletonList("data"));
    }

    @Test
    public void testDesc()
    {
        MaterializedResult result = execute("DESC data");
        MaterializedResult expectedColumns = MaterializedResult
                .resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("time", "timestamp with time zone", "", "")
                .row("tag1", "varchar", "", "")
                .row("tag2", "varchar", "", "")
                .row("field1", "bigint", "", "")
                .row("field2", "double", "", "")
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
                .resultBuilder(session, BIGINT)
                .row(1L)
                .row(3L)
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

    @Test
    public void testTimeBetween()
    {
        MaterializedResult result = execute(String.format("SELECT * FROM data where time between timestamp '%s' and timestamp '%s'",
                SQL_TIMESTAMP_FORMAT.format(TestData.T[0]), SQL_TIMESTAMP_FORMAT.format(TestData.T[2])));
        assertEquals(result, expect(0, 1, 2));
    }

    @Test
    public void testTimeRange()
    {
        MaterializedResult result = execute(String.format("SELECT * FROM data where time > timestamp '%s' and time < timestamp '%s'",
                SQL_TIMESTAMP_FORMAT.format(TestData.T[0]), SQL_TIMESTAMP_FORMAT.format(TestData.T[2])));
        assertEquals(result, expect(1));
    }

    @AfterClass
    @Override
    public void close()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    private synchronized MaterializedResult execute(String sql)
    {
        return queryRunner.getQueryRunner().getClient().execute(session, sql)
                .getResult()
                .toTestTypes();
    }

    private MaterializedResult expect(int... rows)
    {
        MaterializedResult.Builder expected = MaterializedResult
                .resultBuilder(session, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, VARCHAR, BIGINT, DOUBLE);
        for (int row : rows) {
            expected.row(TestData.getColumns(row));
        }
        return expected.build();
    }

    private static class TestData
    {
        private static final String RETENTION_POLICY = "Schema";
        private static final String MEASUREMENT = "Data";
        private static final Instant[] T = new Instant[] {
                Instant.parse("2019-12-10T21:00:04.446Z"),
                Instant.parse("2019-12-10T21:00:20.446Z"),
                Instant.parse("2019-12-10T22:00:04.446Z"),
        };
        private static final String[] TAG1 = new String[] {"a", "b", "d"};
        private static final String[] TAG2 = new String[] {"b", "c", "b"};
        private static final long[] FIELD1 = new long[] {1, 3, 5};
        private static final double[] FIELD2 = new double[] {2, 4, 6};

        private static void init(InfluxQueryRunner queryRunner)
        {
            queryRunner.createRetentionPolicy(RETENTION_POLICY);
            for (int row = 0; row < 3; row++) {
                String line = String.format("%s,tag1=%s,tag2=%s field1=%di,field2=%f %d000000", MEASUREMENT, TAG1[row], TAG2[row], FIELD1[row], FIELD2[row], T[row].toEpochMilli());
                queryRunner.write(RETENTION_POLICY, line);
            }
        }

        private static Object[] getColumns(int row)
        {
            return new Object[] {ZonedDateTime.ofInstant(T[row], ZoneId.of("UTC")), TAG1[row], TAG2[row], FIELD1[row], FIELD2[row]};
        }
    }
}
