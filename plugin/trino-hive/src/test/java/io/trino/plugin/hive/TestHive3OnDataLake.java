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
package io.trino.plugin.hive;

import io.trino.Session;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestHive3OnDataLake
        extends BaseTestHiveOnDataLake
{
    private static final String BUCKET_NAME = "test-hive-insert-overwrite-" + randomNameSuffix();

    public TestHive3OnDataLake()
    {
        super(BUCKET_NAME, new Hive3FlociDataLake(BUCKET_NAME, HiveHadoop.HIVE3_IMAGE));
    }

    @Test
    public void testAcidTableMultipleDereferencesOfSameColumn()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "APPEND")
                .build();
        String testTable = getFullyQualifiedTestTableName("test_acid_dereferences_" + randomNameSuffix());
        assertUpdate("CREATE TABLE " + testTable + " (id bigint, row_column row(nested_a varchar, nested_b varchar, nested_c varchar), flat_column varchar) WITH (format = 'ORC', transactional = true)");
        try {
            assertUpdate(session, "INSERT INTO " + testTable + " VALUES (1, ROW('a1', 'b1', 'c1'), 'flat1'), (2, ROW('a2', 'b2', 'c2'), 'flat2')", 2);

            assertQuery("SELECT row_column.nested_a, row_column.nested_b, flat_column FROM " + testTable, "VALUES ('a1', 'b1', 'flat1'), ('a2', 'b2', 'flat2')");
            assertQuery("SELECT row_column.nested_a, flat_column FROM " + testTable + " WHERE row_column.nested_b = 'b2'", "VALUES ('a2', 'flat2')");
        }
        finally {
            assertUpdate("DROP TABLE " + testTable);
        }
    }
}
