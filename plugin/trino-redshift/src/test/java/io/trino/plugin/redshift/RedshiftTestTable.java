package io.trino.plugin.redshift;

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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;

import java.util.List;

import static io.trino.plugin.redshift.TestingRedshiftServer.REDSHIFT_RETRY_EXECUTOR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class RedshiftTestTable
        extends TestTable
{
    private final QueryRunner queryRunner;
    private final Session session;

    public RedshiftTestTable(QueryRunner queryRunner, String namePrefix, @Language("SQL") String tableDefinition)
    {
        this(queryRunner, queryRunner.getDefaultSession(), namePrefix, tableDefinition, ImmutableList.of());
    }

    public RedshiftTestTable(QueryRunner queryRunner, Session session, String namePrefix, @Language("SQL") String tableDefinition)
    {
        this(queryRunner, session, namePrefix, tableDefinition, ImmutableList.of());
    }

    public RedshiftTestTable(QueryRunner queryRunner, String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        this(queryRunner, queryRunner.getDefaultSession(), namePrefix, tableDefinition, rowsToInsert);
    }

    public RedshiftTestTable(QueryRunner queryRunner, Session session, String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        // Pass in a no-op SqlExecutor in order to override the table creation functionality here
        super(_ -> {}, namePrefix, tableDefinition, rowsToInsert);
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.session = requireNonNull(session, "session is null");

        REDSHIFT_RETRY_EXECUTOR.run(context -> {
            if ((context.getAttemptCount() > 1)) {
                // Drop the table if it was created in a previous attempt and not cleaned up
                queryRunner.execute(session, format("DROP TABLE IF EXISTS %s", name));
            }
            queryRunner.execute(session, format("CREATE TABLE %s %s", name, tableDefinition));
        });
        try {
            if (!rowsToInsert.isEmpty()) {
                executeWithRetries(format("INSERT INTO %s VALUES %s", name, rowsToInsert.stream()
                        .map("(%s)"::formatted)
                        .collect(joining(", "))));
            }
        }
        catch (Exception e) {
            try (TestTable ignored = this) {
                throw e;
            }
        }
    }

    @Override
    public void close()
    {
        executeWithRetries("DROP TABLE " + name);
    }

    private void executeWithRetries(@Language("SQL") String sql)
    {
        REDSHIFT_RETRY_EXECUTOR.run(() -> queryRunner.execute(session, sql));
    }
}
