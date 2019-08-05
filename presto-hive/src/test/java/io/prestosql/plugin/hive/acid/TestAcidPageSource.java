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
package io.prestosql.plugin.hive.acid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.acid.AcidNationRow.getExpectedResult;
import static io.prestosql.plugin.hive.acid.AcidNationRow.readFileCols;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/*
 * Tests reading file with multiple row groups and multiple stripes
 * This test reads Acid file nationFile25kRowsSortedOnNationKey/bucket_00000
 * It has replicated version of 25 rows of nations replicated into 25000 rows
 * It has row group size = 1000 and has content in sorted order of nationkey
 * Hence first row group has all nationkey=0 rows, next has nationkey=1 and so on
 *
 * This file has 5 stripes and 25 rowgroups
 *
 * Table Schema: n_nationkey:int,n_name:string,n_regionkey:int,n_comment:string
 *
 * Actual File Schema:
 * struct<operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,currentTransaction:bigint,row:struct<n_nationkey:int,n_name:string,n_regionkey:int,n_comment:string>>
 *
 */
public class TestAcidPageSource
{
    private String filename = "nationFile25kRowsSortedOnNationKey/bucket_00000";
    private List<String> columnNames = ImmutableList.of("n_nationkey", "n_name", "n_regionkey", "n_comment");
    private List<Type> columnTypes = ImmutableList.of(IntegerType.INTEGER, VarcharType.VARCHAR, IntegerType.INTEGER, VarcharType.VARCHAR);

    @Test
    public void testFullFileRead()
            throws IOException
    {
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes);
        List<AcidNationRow> rows = readFullFile(pageSource, true);

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.empty());
        assertEquals(expected, rows);
    }

    @Test
    public void testSingleColumnRead()
            throws IOException
    {
        int colToRead = 2;
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, ImmutableList.of(columnNames.get(colToRead)), ImmutableList.of(columnTypes.get(colToRead)));
        List<AcidNationRow> rows = readFileCols(pageSource, ImmutableList.of(columnNames.get(colToRead)), ImmutableList.of(columnTypes.get(colToRead)), true);

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.of(colToRead));
        assertEquals(expected, rows);
    }

    @Test
    /*
     * tests file stats based pruning works fine
     */
    public void testFullFileSkipped()
    {
        Domain nonExistingDomain = Domain.create(ValueSet.of(IntegerType.INTEGER, 100L), false);
        HiveColumnHandle nationKeyColumnHandle = new HiveColumnHandle(
                columnNames.get(0),
                HiveType.toHiveType(new HiveTypeTranslator(), columnTypes.get(0)),
                columnTypes.get(0).getTypeSignature(),
                0,
                REGULAR,
                Optional.empty());
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(nationKeyColumnHandle, nonExistingDomain));

        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes, tupleDomain);
        List<AcidNationRow> rows = readFullFile(pageSource, true);

        assertTrue(rows.size() == 0);
        assertTrue(((OrcPageSource) pageSource).getRecordReader().isFileSkipped());
    }

    @Test
    /*
     * Tests stripe stats and row groups stats based pruning works fine
     */
    public void testSomeStripesAndRowGroupRead()
            throws IOException
    {
        Domain nonExistingDomain = Domain.create(ValueSet.of(IntegerType.INTEGER, 0L), false);
        HiveColumnHandle nationKeyColumnHandle = new HiveColumnHandle(
                columnNames.get(0),
                HiveType.toHiveType(new HiveTypeTranslator(), columnTypes.get(0)),
                columnTypes.get(0).getTypeSignature(),
                0,
                REGULAR,
                Optional.empty());
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(nationKeyColumnHandle, nonExistingDomain));

        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes, tupleDomain);
        List<AcidNationRow> rows = readFullFile(pageSource, true);

        List<AcidNationRow> expected = getExpectedResult(Optional.of(0), Optional.empty());
        assertTrue(rows.size() != 0);
        assertFalse(((OrcPageSource) pageSource).getRecordReader().isFileSkipped());
        assertTrue(((OrcPageSource) pageSource).getRecordReader().stripesRead() == 1);  // 1 out of 5 stripes should be read
        assertTrue(((OrcPageSource) pageSource).getRecordReader().getRowGroupsRead() == 1); // 1 out of 25 rowgroups should be read
        assertEquals(expected, rows);
    }

    private List<AcidNationRow> readFullFile(ConnectorPageSource pageSource, boolean resultsNeeded)
    {
        return readFileCols(pageSource, columnNames, columnTypes, resultsNeeded);
    }
}
