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
package io.prestosql.plugin.iceberg;

import org.testng.annotations.Test;

import static org.apache.iceberg.FileFormat.AVRO;

public class TestIcebergAvroSmoke
        extends AbstractTestIcebergSmoke
{
    public TestIcebergAvroSmoke()
    {
        super(AVRO);
    }

    @Override
    @Test
    public void testHourTransform()
    {
        testHourTransform(false);
    }

    @Override
    @Test
    public void testDayTransformDate()
    {
        testDayTransformDate(false);
    }

    @Override
    @Test
    public void testDayTransformTimestamp()
    {
        testDayTransformTimestamp(false);
    }

    @Override
    @Test
    public void testMonthTransformDate()
    {
        testMonthTransformDate(false);
    }

    @Override
    @Test
    public void testMonthTransformTimestamp()
    {
        testMonthTransformTimestamp(false);
    }

    @Override
    @Test
    public void testYearTransformDate()
    {
        testYearTransformDate(false);
    }

    @Override
    @Test
    public void testYearTransformTimestamp()
    {
        testYearTransformTimestamp(false);
    }

    @Override
    @Test
    public void testTruncateTransform()
    {
        testTruncateTransform(false);
    }

    @Override
    @Test
    public void testBucketTransform()
    {
        testBucketTransform(false);
    }

    @Override
    @Test
    public void testBasicTableStatistics()
    {
        testBasicTableStatistics(false);
    }

    @Override
    @Test
    public void testMultipleColumnTableStatistics()
    {
        testMultipleColumnTableStatistics(false);
    }

    @Override
    @Test
    public void testPartitionedTableStatistics()
    {
        testPartitionedTableStatistics(false);
    }

    @Override
    @Test
    public void testStatisticsConstraints()
    {
    }
}
