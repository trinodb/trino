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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMerge
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("schema");

        LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());
        queryRunner.createCatalog(
                "mock",
                MockConnectorFactory.builder()
                        .withGetTableHandle((session, schemaTableName) -> {
                            if (schemaTableName.getTableName().equals("schemaTable")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            if (schemaTableName.getTableName().equals("t")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            return null;
                        })
                        .withGetColumns(name -> ImmutableList.of(
                                new ColumnMetadata("column1", INTEGER),
                                new ColumnMetadata("column2", INTEGER)))
                        .withRowChangeParadigm(RowChangeParadigm.UPDATE_PARTIAL_COLUMNS)
                        .build(),
                ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testUpdate()
    {
        assertPlan(
                "UPDATE t SET column1 = column1 + 11",
                anyTree(tableScan("t")
                        // merge_row_id, column1
                        .withNumberOfOutputColumns(2)));
    }

    @Test
    public void testDelete()
    {
        assertPlan(
                "DELETE from t WHERE column1 = 11",
                anyTree(tableScan("t")
                        // merge_row_id, column1
                        .withNumberOfOutputColumns(2)));
    }
}
