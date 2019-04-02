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
package io.prestosql.sql.planner.planPrinter;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.MethodHandleUtil;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.TestLocalQueries;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.StructuralTestUtil.mapType;

public class TestPlanPrinterUtil
        extends AbstractTestQueryFramework
{
    public TestPlanPrinterUtil()
    {
        super(TestLocalQueries::createLocalQueryRunner);
    }

    @Test(expectedExceptions = OperatorNotFoundException.class)
    public void testCastArrayToVarcharFails()
    {
        Type type = new ArrayType(IntegerType.INTEGER);
        Object value = new IntArrayBlock(3, java.util.Optional.empty(), new int[] {1, 2, 3});
        Session session = getSession();
        Metadata metadata = getQueryRunner().getMetadata();
        PlanPrinterUtil.castToVarcharOrFail(type, value, metadata.getFunctionRegistry(), session);
    }

    @Test(expectedExceptions = OperatorNotFoundException.class)
    public void testCastMapToVarcharFails()
    {
        MapType type = new MapType(
                BIGINT,
                VARCHAR,
                MethodHandleUtil.methodHandle(TestPlanPrinterUtil.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestPlanPrinterUtil.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestPlanPrinterUtil.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestPlanPrinterUtil.class, "throwUnsupportedOperation"));
        Object value = createBlockWithValuesFromKeyValueBlock(createTestMap(1));
        Session session = getSession();
        Metadata metadata = getQueryRunner().getMetadata();
        PlanPrinterUtil.castToVarcharOrFail(type, value, metadata.getFunctionRegistry(), session);
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }

    private Map<String, Long>[] createTestMap(int... entryCounts)
    {
        Map<String, Long>[] result = new Map[entryCounts.length];
        for (int rowNumber = 0; rowNumber < entryCounts.length; rowNumber++) {
            int entryCount = entryCounts[rowNumber];
            Map<String, Long> map = new HashMap<>();
            for (int entryNumber = 0; entryNumber < entryCount; entryNumber++) {
                map.put("key" + entryNumber, entryNumber == 5 ? null : rowNumber * 100L + entryNumber);
            }
            result[rowNumber] = map;
        }
        return result;
    }

    private Block createBlockWithValuesFromKeyValueBlock(Map<String, Long>[] maps)
    {
        List<String> keys = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        int[] offsets = new int[maps.length + 1];
        boolean[] mapIsNull = new boolean[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<String, Long> map = maps[i];
            mapIsNull[i] = map == null;
            if (map == null) {
                offsets[i + 1] = offsets[i];
            }
            else {
                for (Map.Entry<String, Long> entry : map.entrySet()) {
                    keys.add(entry.getKey());
                    values.add(entry.getValue());
                }
                offsets[i + 1] = offsets[i] + map.size();
            }
        }
        return mapType(VARCHAR, BIGINT).createBlockFromKeyValue(Optional.of(mapIsNull), offsets, createStringsBlock(keys), createLongsBlock(values));
    }
}
