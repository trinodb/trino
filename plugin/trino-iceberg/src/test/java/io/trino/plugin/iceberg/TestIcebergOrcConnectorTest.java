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
package io.trino.plugin.iceberg;

import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergOrcConnectorTest
        extends BaseIcebergConnectorTest
{
    public TestIcebergOrcConnectorTest()
    {
        super(ORC);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary") &&
                !typeName.equalsIgnoreCase("uuid");
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkOrcFileSorting(path, sortColumnName);
    }

    @Test
    public void testTinyintType()
            throws Exception
    {
        testReadSingleIntegerColumnOrcFile(101, "single-tinyint-column.orc", 127);
    }

    @Test
    public void testSmallintType()
            throws Exception
    {
        testReadSingleIntegerColumnOrcFile(31234, "single-smallint-column.orc", 32767);
    }

    private void testReadSingleIntegerColumnOrcFile(int initialTableValue, String orcFileResourceName, int expectedValue)
            throws Exception
    {
        checkArgument(initialTableValue != expectedValue); // Needs to be close to expected so that generated file size matches the one from test resources
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_read_as_integer", "(\"_col0\") AS VALUES " + initialTableValue + ", NULL")) {
            Path orcFilePath = Path.of((String) computeScalar(format("SELECT DISTINCT file_path FROM \"%s$files\"", table.getName())));
            Files.copy(new File(getResource(orcFileResourceName).toURI()).toPath(), orcFilePath, REPLACE_EXISTING);
            Files.delete(orcFilePath.resolveSibling(format(".%s.crc", orcFilePath.getFileName())));

            assertThat(query("TABLE " + table.getName()))
                    .matches("VALUES NULL, " + expectedValue);
        }
    }

    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        // TODO https://github.com/trinodb/trino/issues/16273 The connector can't read row types having ambiguous field names in ORC files. e.g. row(X int, x int)
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessageContaining("Error opening Iceberg split")
                .hasStackTraceContaining("Multiple entries with same key");
    }
}
