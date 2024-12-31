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
package io.trino.plugin.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InstantiationUtil;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.data.BinaryString.fromString;

/**
 * presto test util.
 */
public class PaimonTestUtils
{
    private PaimonTestUtils() {}

    public static byte[] getSerializedTable()
            throws Exception
    {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        Path tablePath = new Path(warehouse, "test.db/user");
        SimpleTableTestHelper testHelper = createTestHelper(tablePath);
        testHelper.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper.write(
                GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper.commit();
        Map<String, String> config = new HashMap<>();
        config.put("warehouse", warehouse);
        Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(config)));
        Identifier tablePath2 = new Identifier("test", "user");
        return InstantiationUtil.serializeObject(catalog.getTable(tablePath2));
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath)
            throws Exception
    {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new BigIntType()),
                                // test field name has upper case
                                new DataField(2, "aCa", new VarCharType()),
                                new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }
}
