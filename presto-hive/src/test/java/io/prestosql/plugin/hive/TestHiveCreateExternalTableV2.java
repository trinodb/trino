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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public class TestHiveCreateExternalTableV2
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
            .setHiveProperties(ImmutableMap.of(
                "hive.non-managed-table-creates-enabled", "false",
                "hive.non-managed-table-creates-v2-enabled", "true"))
            .setInitialTables(ImmutableList.of(ORDERS, CUSTOMER))
            .build();
    }

    @Test
    public void testCreateExternalTableFailure()
            throws IOException
    {
        File tempDir = createTempDir();

        @Language("SQL") String createTableSql = format("" +
                "CREATE TABLE %s.%s.test_create_external (\n" +
                "   action varchar,\n" +
                "   name varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   external_location = '%s',\n" +
                "   format = 'TEXTFILE',\n" +
                "   textfile_field_separator = U&'\\0001'\n" +
                ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertQueryFails(createTableSql, "The schema location must be a prefix of the table external location");

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateExternalTable()
            throws IOException
    {
        String schemaName = "foo";
        String tableName = "bar";
        File tempDir = createTempDir();
        File tempSubDir = new File(tempDir.toURI().toASCIIString() + File.separator + tableName);
        tempSubDir.mkdir();

        @Language("SQL") String createTableSql = format("" +
                "CREATE TABLE %s.%s.test_create_external (\n" +
                "   action varchar,\n" +
                "   name varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   external_location = '%s',\n" +
                "   format = 'TEXTFILE',\n" +
                "   textfile_field_separator = U&'\\0001'\n" +
                ")",
                getSession().getCatalog().get(),
                schemaName,
                new Path(tempSubDir.toURI().toASCIIString()).toString());

        assertUpdate(createTableSql);

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }
}
