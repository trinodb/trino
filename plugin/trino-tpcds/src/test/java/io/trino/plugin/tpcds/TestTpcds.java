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
package io.trino.plugin.tpcds;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalTime;

import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTpcds
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpcdsQueryRunner.builder().build();
    }

    @Test
    public void testSelect()
    {
        MaterializedResult actual = computeActual(
                "SELECT c_first_name, c_last_name, ca_address_sk, ca_gmt_offset " +
                        "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk " +
                        "WHERE ca_address_sk = 4");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                // note that c_first_name and c_last_name are both of type CHAR(X) so the results
                // are padded with whitespace
                .row("James               ", "Brown                         ", 4L, new BigDecimal("-7.00"))
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);

        actual = computeActual(
                "SELECT c_first_name, c_last_name " +
                        "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk " +
                        "WHERE ca_address_sk = 4 AND ca_gmt_offset = DECIMAL '-7.00'");
        expected = resultBuilder(getSession(), actual.getTypes())
                .row("James               ", "Brown                         ")
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testTimeRepresentation()
    {
        LocalTime timeNow = LocalTime.now();
        LocalTime readTime = (LocalTime) computeScalar("SELECT dv_create_time FROM dbgen_version");
        long differenceNanos = (NANOSECONDS_PER_DAY + readTime.toNanoOfDay() - timeNow.toNanoOfDay()) % NANOSECONDS_PER_DAY;
        differenceNanos = Math.min(differenceNanos, NANOSECONDS_PER_DAY - differenceNanos);
        assertThat(differenceNanos)
                .isBetween(0L, 10 * NANOSECONDS_PER_SECOND);
    }

    @Test
    public void testLargeInWithShortDecimal()
    {
        // TODO add a test with long decimal
        String longValues = range(0, 5000)
                .map(value -> value * 2) // Make the values discontinuous to avoid getting optimized to a BETWEEN filter
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertQuery("SELECT typeof(i_current_price) FROM item LIMIT 1", "VALUES 'decimal(7,2)'"); // decimal(7,2) is a short decimal
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price IN (" + longValues + ")");
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price NOT IN (" + longValues + ")");
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price IN (i_wholesale_cost, " + longValues + ")");
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price NOT IN (i_wholesale_cost, " + longValues + ")");
    }

    @Test
    public void testShowTables()
    {
        assertQuerySucceeds(createSession("sf1"), "SHOW TABLES");
        assertQuerySucceeds(createSession("sf1.0"), "SHOW TABLES");
        assertQuerySucceeds("SHOW TABLES FROM sf1");
        assertQuerySucceeds("SHOW TABLES FROM \"sf1.0\"");
        assertQueryFails("SHOW TABLES FROM sf0", "line 1:1: Schema 'sf0' does not exist");
    }

    @Test
    public void testDateColumnValuesCorrectness()
    {
        // make sure date values are correct regardless of the system timezone selected (test are executed with the system timezone set to America/Bahia_Banderas)
        assertQuery("SELECT d_date FROM date_dim WHERE d_date_id = 'AAAAAAAAOKJNECAA'", "SELECT DATE '1900-01-02'");
    }

    private Session createSession(String schemaName)
    {
        return testSessionBuilder()
                .setSource("test")
                .setCatalog("tpcds")
                .setSchema(schemaName)
                .build();
    }
}
