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
package io.trino.plugin.sybase;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class SybaseDataLoader
{
    private static final Logger log = Logger.get(SybaseDataLoader.class);

    private SybaseDataLoader() {}

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    public static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        copyTable(queryRunner, table, session);
    }

    public static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        log.info("Initialising tables...");
        initTable(queryRunner, table);
        log.info("Running import for %s", table.getObjectName());
        @Language("SQL") String sql = format("INSERT INTO %s SELECT * FROM %s", table.getObjectName(), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());

        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + table).getOnlyValue())
                .as("Table is not loaded properly: %s", table)
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + table.getObjectName()).getOnlyValue());
    }

    private static void initTable(QueryRunner queryRunner, QualifiedObjectName table)
    {
        String createDdl = "CREATE TABLE IF NOT EXISTS " + table.getObjectName() + " (%s)";
        AtomicReference<String> schemaStr = new AtomicReference<>();
        queryRunner.execute("describe " + table).getMaterializedRows().forEach(row -> {
            String colName = (String) row.getField(0);
            String dataType = (String) row.getField(1);
            schemaStr.set(schemaStr + " " + colName + " " + dataType + ",");
        });

        String schema = schemaStr.get().substring(5, schemaStr.get().length() - 1);
        @Language("SQL") String sqlToExecute = String.format(createDdl, schema);
        queryRunner.execute(sqlToExecute);
    }
}
