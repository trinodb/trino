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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestHiveParquetTypeWidenedPredicate
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder().build();
    }

    @Test
    public void testPredicateOnFloatColumnWidenedToDouble()
    {
        String source = "source_float_" + randomNameSuffix();
        String widened = "widened_double_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (c real) WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['c'])");
        try {
            assertUpdate("INSERT INTO " + source + " VALUES REAL '1.5', REAL '2.5'", 2);
            String location = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + source);
            assertUpdate(format("CREATE TABLE %s (c double) WITH (format = 'PARQUET', external_location = '%s')", widened, location));
            try {
                assertQuery("SELECT count(*) FROM " + widened, "VALUES 2");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = DOUBLE '1.5'", "VALUES 1");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = DOUBLE '9.9'", "VALUES 0");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + widened);
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + source);
        }
    }

    @Test
    public void testPredicateOnIntColumnWidenedToDouble()
    {
        String source = "source_int_double_" + randomNameSuffix();
        String widened = "widened_int_double_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (c integer) WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['c'])");
        try {
            assertUpdate("INSERT INTO " + source + " VALUES 1, 2", 2);
            String location = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + source);
            assertUpdate(format("CREATE TABLE %s (c double) WITH (format = 'PARQUET', external_location = '%s')", widened, location));
            try {
                assertQuery("SELECT count(*) FROM " + widened, "VALUES 2");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = DOUBLE '1'", "VALUES 1");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = DOUBLE '9'", "VALUES 0");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + widened);
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + source);
        }
    }

    @Test
    public void testPredicateOnIntColumnWidenedToBigintWithBloomFilter()
    {
        String source = "source_int_" + randomNameSuffix();
        String widened = "widened_bigint_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (c integer) WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['c'])");
        try {
            assertUpdate("INSERT INTO " + source + " VALUES 123, 456", 2);
            String location = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + source);
            assertUpdate(format("CREATE TABLE %s (c bigint) WITH (format = 'PARQUET', external_location = '%s')", widened, location));
            try {
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = BIGINT '123'", "VALUES 1");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = BIGINT '999'", "VALUES 0");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + widened);
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + source);
        }
    }

    @Test
    public void testPredicateOnBigintColumnWidenedToDouble()
    {
        String source = "source_bigint_double_" + randomNameSuffix();
        String widened = "widened_bigint_double_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (c bigint) WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['c'])");
        try {
            assertUpdate("INSERT INTO " + source + " VALUES BIGINT '1', BIGINT '2'", 2);
            String location = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + source);
            assertUpdate(format("CREATE TABLE %s (c double) WITH (format = 'PARQUET', external_location = '%s')", widened, location));
            try {
                assertQuery("SELECT count(*) FROM " + widened, "VALUES 2");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = DOUBLE '1'", "VALUES 1");
                assertQuery("SELECT count(*) FROM " + widened + " WHERE c = DOUBLE '9'", "VALUES 0");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + widened);
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + source);
        }
    }
}
