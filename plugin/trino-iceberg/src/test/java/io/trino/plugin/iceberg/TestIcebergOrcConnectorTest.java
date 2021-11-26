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

import io.trino.Session;
import org.testng.annotations.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
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
        return !(typeName.equalsIgnoreCase("varbinary")) &&
                !(typeName.equalsIgnoreCase("uuid"));
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "10")
                .build();
    }

    @Override
    @Test
    public void testDeleteWithBigintEqualityPredicate()
    {
        assertThatThrownBy(super::testDeleteWithBigintEqualityPredicate)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDeleteWithVarcharInequalityPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharInequalityPredicate)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDeleteWithVarcharGreaterAndLowerPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharGreaterAndLowerPredicate)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testUpdateUnpartitionedTable()
    {
        assertThatThrownBy(super::testUpdateUnpartitionedTable)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testUpdatePartitionedTable()
    {
        assertThatThrownBy(super::testUpdatePartitionedTable)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testUpdatePartitionedBucketedTable()
    {
        assertThatThrownBy(super::testUpdatePartitionedBucketedTable)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }

    @Override
    @Test
    public void testUpdatePartitionedMonthlyTable()
    {
        assertThatThrownBy(super::testUpdatePartitionedMonthlyTable)
                .hasStackTraceContaining("Row level delete and update are not supported for ORC type");
    }
}
