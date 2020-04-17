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
package io.prestosql.tests.hive;

import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCreateDropSchema
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Test
    public void testCreateDropSchema()
            throws SQLException
    {
        onHive().executeQuery("DROP DATABASE IF EXISTS test_drop_schema CASCADE");

        onPresto().executeQuery("CREATE SCHEMA test_drop_schema");
        assertTrue(hdfsClient.exist("/user/hive/warehouse/test_drop_schema.db"));

        onPresto().executeQuery("CREATE TABLE test_drop_schema.test_drop (col1 int)");
        assertThat(() -> query("DROP SCHEMA test_drop_schema"))
                .failsWithMessage("Schema not empty: test_drop_schema");

        onPresto().executeQuery("DROP TABLE test_drop_schema.test_drop");

        onPresto().executeQuery("DROP SCHEMA test_drop_schema");
        assertFalse(hdfsClient.exist("/user/hive/warehouse/test_drop_schema.db"));
    }
}
