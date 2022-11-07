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

import com.mongodb.client.MongoClient;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.bson.Document;

import java.util.List;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MongoCreateAndInsertDataSetup
        implements DataSetup
{
    private final TrinoSqlExecutor trinoSqlExecutor;
    private final MongoClient mongoClient;
    private final String databaseName;
    private final String tableNamePrefix;

    public MongoCreateAndInsertDataSetup(TrinoSqlExecutor trinoSqlExecutor, MongoClient mongoClient, String databaseName, String tableNamePrefix)
    {
        this.trinoSqlExecutor = requireNonNull(trinoSqlExecutor, "trinoSqlExecutor is null");
        this.mongoClient = requireNonNull(mongoClient, "mongoClient is null");
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableNamePrefix = requireNonNull(tableNamePrefix, "tableNamePrefix is null");
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TestTable testTable = new MongoTestTable(trinoSqlExecutor, tableNamePrefix);
        try {
            insertRows(testTable, inputs);
        }
        catch (Exception e) {
            closeAllSuppress(e, testTable);
            throw e;
        }
        return testTable;
    }

    private void insertRows(TestTable testTable, List<ColumnSetup> inputs)
    {
        int i = 0;
        StringBuilder json = new StringBuilder("{");
        for (ColumnSetup columnSetup : inputs) {
            json.append(format("col_%d: ", i++));
            json.append(columnSetup.getInputLiteral());
            if (i != inputs.size()) {
                json.append(",");
            }
        }
        json.append("}");
        mongoClient.getDatabase(databaseName).getCollection(testTable.getName()).insertOne(Document.parse(json.toString()));
    }
}
