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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IpAddressType.IPADDRESS;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class TestFlatHashStrategy
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(TYPE_OPERATORS);

    @Test
    public void testBatchedRawHashesMatchSinglePositionHashes()
    {
        List<Type> types = createTestingTypes();
        FlatHashStrategy flatHashStrategy = JOIN_COMPILER.getFlatHashStrategy(types);

        int positionCount = 1024;
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = createRandomBlockForType(types.get(i), positionCount, 0.25f);
        }

        long[] hashes = new long[positionCount];
        flatHashStrategy.hashBlocksBatched(blocks, hashes, 0, positionCount);
        for (int position = 0; position < hashes.length; position++) {
            long singleRowHash = flatHashStrategy.hash(blocks, position);
            if (hashes[position] != singleRowHash) {
                fail("Hash mismatch: %s <> %s at position %s - Values: %s".formatted(hashes[position], singleRowHash, position, singleRowTypesAndValues(types, blocks, position)));
            }
        }
        // Ensure the formatting logic produces a real string and doesn't blow up since otherwise this code wouldn't be exercised
        assertNotNull(singleRowTypesAndValues(types, blocks, 0));
    }

    private static List<Type> createTestingTypes()
    {
        List<Type> baseTypes = List.of(
                BIGINT,
                BOOLEAN,
                createCharType(5),
                createDecimalType(18),
                createDecimalType(38),
                DOUBLE,
                INTEGER,
                IPADDRESS,
                REAL,
                TIMESTAMP_SECONDS,
                TIMESTAMP_MILLIS,
                TIMESTAMP_MICROS,
                TIMESTAMP_NANOS,
                TIMESTAMP_PICOS,
                UUID,
                VARBINARY,
                VARCHAR);

        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        builder.addAll(baseTypes);
        builder.add(RowType.anonymous(baseTypes));
        for (Type baseType : baseTypes) {
            builder.add(new ArrayType(baseType));
            builder.add(new MapType(baseType, baseType, TYPE_OPERATORS));
        }
        return builder.build();
    }

    private static String singleRowTypesAndValues(List<Type> types, Block[] blocks, int position)
    {
        ConnectorSession connectorSession = TestingSession.testSessionBuilder().build().toConnectorSession();
        StringBuilder builder = new StringBuilder();
        int column = 0;
        for (Type type : types) {
            builder.append("\n\t");
            builder.append(type);
            builder.append(": ");
            builder.append(type.getObjectValue(connectorSession, blocks[column], position));
            column++;
        }
        return builder.toString();
    }
}
