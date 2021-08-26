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
package io.trino.plugin.mongodb;

import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseMongoDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected boolean supportsCreateSchema()
    {
        return false;
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected boolean supportsRenameTable()
    {
        return false;
    }

    @Override
    public void testRenameColumn()
    {
        // the connector does not support renaming columns
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("test disabled for Mongo");
    }

    @Override
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        if (columnName.equals("a.dot")) {
            // TODO (https://github.com/trinodb/trino/issues/3460)
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasStackTraceContaining("TableWriterOperator") // during INSERT
                    .hasMessage("Invalid BSON field name a.dot");
            throw new SkipException("Insert would fail");
        }

        super.testColumnName(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
