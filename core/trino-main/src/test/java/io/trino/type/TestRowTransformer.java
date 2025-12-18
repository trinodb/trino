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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.RowTransformer.appendNativeValue;
import static io.trino.spi.block.RowTransformer.build;
import static io.trino.spi.block.RowTransformer.setNativeValue;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRowTransformer
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestRowTransformer.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @SqlNullable
    @SqlType("row(name varchar, greeting varchar)")
    @ScalarFunction("transform_single_row")
    public static SqlRow transformSingleRow(
            @TypeParameter("row(name varchar, greeting varchar)") RowType rowType,
            @SqlType("row(name varchar, greeting varchar)") SqlRow sqlRow)
    {
        return build(rowType, sqlRow, transformer -> {
            // Set the name field to "anonymous"
            transformer.transform(setNativeValue("anonymous"), "name");

            // Append " world" to the greeting
            transformer.transform((type, original) -> {
                if (readNativeValue(type, original, 0) instanceof Slice originalValue) {
                    Slice slice = utf8Slice(originalValue.toStringUtf8() + " world");
                    return writeNativeValue(type, slice);
                }
                return original;
            }, "greeting");
        });
    }

    @SqlNullable
    @SqlType("row(age integer, name array(varchar))")
    @ScalarFunction("transform_single_row_varchar_array_add")
    public static SqlRow transformSingleRowVarcharArrayAdd(
            @TypeParameter("row(age integer, name array(varchar))") RowType rowType,
            @SqlType("row(age integer, name array(varchar))") SqlRow sqlRow)
    {
        return build(rowType, sqlRow, transformer -> {
            transformer.transform(appendNativeValue("john"), "name");
        });
    }

    @SqlNullable
    @TypeParameter("E")
    @SqlType("array(E)")
    @ScalarFunction("transform_array")
    public static Block transformArray(
            @TypeParameter("E") RowType rowType,
            @SqlType("array(E)") Block input)
    {
        return build(rowType, (RowBlock) input, transformer -> {
            transformer.transform(setNativeValue("goodbye"), "greeting");
        });
    }

    @SqlNullable
    @TypeParameter("E")
    @SqlType("array(E)")
    @ScalarFunction("transform_single_row_set_varchar")
    public static Block setVarchar(
            @TypeParameter("E") RowType rowType,
            @SqlType("array(E)") Block input,
            @SqlType("varchar") Slice value)
    {
        if (input instanceof final RowBlock rowBlock) {
            return build(rowType, rowBlock, transformer -> {
                transformer.transform(setNativeValue(value), "first", "second", "third");
            });
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Expected RowBlock but was " + input.getClass().getSimpleName());
    }

    @Test
    public void testTransformSingleRow()
    {
        assertThat(assertions.expression("transform_single_row(cast(row('bob', 'hi') as row(name varchar, greeting varchar)))"))
                .hasType(rowType(field("name", VARCHAR), field("greeting", VARCHAR)))
                .isEqualTo(ImmutableList.of("anonymous", "hi world"));
    }

    @Test
    public void testTransformSingleRowVarcharArrayAdd()
    {
        assertThat(assertions.expression("transform_single_row_varchar_array_add(cast(row(36, array['don', 'thomas']) as row(age integer, name array(varchar))))"))
                .hasType(rowType(field("age", IntegerType.INTEGER), field("name", new ArrayType(VARCHAR))))
                .isEqualTo(ImmutableList.of(36, ImmutableList.of("don", "thomas", "john")));
    }

    @Test
    public void testTransformArray()
    {
        assertThat(assertions.expression("transform_array(cast(ARRAY[row('bob', 'hi'), row('emma', 'hello')] as array(row(name varchar, greeting varchar))))"))
                .hasType(new ArrayType(rowType(field("name", VARCHAR), field("greeting", VARCHAR))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("bob", "goodbye"), ImmutableList.of("emma", "goodbye")));
    }

    @Test
    public void testTransformArrayNested()
    {
        assertThat(assertions.expression("""
                transform_single_row_set_varchar(
                    cast(
                        ARRAY[row('1.1', row('1.2.1', row('1.2.2.1', 'original')))]
                    as
                        array(row(id1 varchar, first row(id2 varchar, second row(id3 varchar, third varchar))))
                    ),
                    'updated')
                """))
                .hasType(new ArrayType(
                        rowType(
                                field("id1", VARCHAR),
                                field("first", rowType(
                                        field("id2", VARCHAR),
                                        field("second", rowType(
                                                field("id3", VARCHAR),
                                                field("third", VARCHAR))))))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of(
                                "1.1",
                                ImmutableList.of(
                                        "1.2.1",
                                        ImmutableList.of(
                                                "1.2.2.1",
                                                "updated")))));
    }

    @Test
    public void testTransformArrayNestedRowArray()
    {
        assertThat(assertions.expression("""
                transform_single_row_set_varchar(
                    cast(
                        ARRAY[row('1.1', ARRAY[row('1.2[1].1', row('1.2[1].2.1', 'original')), row('1.2[2].1', row('1.2[2].2.1', 'original'))])]
                    as
                        array(row(id1 varchar, first array(row(id2 varchar, second row(id3 varchar, third varchar)))))
                    ),
                    'updated')
                """))
                .hasType(new ArrayType(
                        rowType(
                                field("id1", VARCHAR),
                                field("first", new ArrayType(rowType(
                                        field("id2", VARCHAR),
                                        field("second", rowType(
                                                field("id3", VARCHAR),
                                                field("third", VARCHAR)))))))))
                .isEqualTo(ImmutableList.of(
                        ImmutableList.of(
                                "1.1",
                                ImmutableList.of(
                                        ImmutableList.of(
                                                "1.2[1].1",
                                                ImmutableList.of(
                                                        "1.2[1].2.1",
                                                        "updated")),
                                        ImmutableList.of(
                                                "1.2[2].1",
                                                ImmutableList.of(
                                                        "1.2[2].2.1",
                                                        "updated"))))));
    }
}
