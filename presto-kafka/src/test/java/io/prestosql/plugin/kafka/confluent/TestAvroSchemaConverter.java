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
package io.prestosql.plugin.kafka.confluent;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.kafka.confluent.AvroSchemaConverter.DUMMY_FIELD_NAME;
import static io.prestosql.plugin.kafka.confluent.AvroSchemaConverter.EmptyFieldStrategy.ADD_DUMMY;
import static io.prestosql.plugin.kafka.confluent.AvroSchemaConverter.EmptyFieldStrategy.FAIL;
import static io.prestosql.plugin.kafka.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestAvroSchemaConverter
{
    private static final String RECORD_NAME = "test";

    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();

    @Test
    public void testConvertSchema()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("bool_col").type().booleanType().noDefault()
                .name("int_col").type().intType().noDefault()
                .name("long_col").type().longType().noDefault()
                .name("float_col").type().floatType().noDefault()
                .name("double_col").type().doubleType().noDefault()
                .name("string_col").type().stringType().noDefault()
                .name("enum_col").type().enumeration("colors").symbols("blue", "red", "yellow").noDefault()
                .name("bytes_col").type().bytesType().noDefault()
                .name("fixed_col").type().fixed("fixed").size(5).noDefault()
                .name("union_col").type().unionOf().nullType().and().floatType().and().doubleType().endUnion().noDefault()
                .name("union_col2").type().unionOf().nullType().and().intType().and().longType().endUnion().noDefault()
                .name("union_col3").type().unionOf().nullType().and().bytesType().and().type("fixed").endUnion().noDefault()
                .name("union_col4").type().unionOf().nullType().and().type("colors").and().stringType().endUnion().noDefault()
                .name("list_col").type().array().items().intType().noDefault()
                .name("map_col").type().map().values().intType().noDefault()
                .name("record_col").type().record("record_col")
                    .fields()
                    .name("nested_list").type().array().items().map().values().stringType().noDefault()
                    .name("nested_map").type().map().values().array().items().stringType().noDefault()
                    .endRecord()
                    .noDefault()
                .endRecord();
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(new TestingTypeManager(), IGNORE);
        List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
        List<Type> expected = ImmutableList.<Type>builder()
                .add(BOOLEAN)
                .add(INTEGER)
                .add(BIGINT)
                .add(REAL)
                .add(DOUBLE)
                .add(VARCHAR)
                .add(VARCHAR)
                .add(VARBINARY)
                .add(VARBINARY)
                .add(DOUBLE)
                .add(BIGINT)
                .add(VARBINARY)
                .add(VARCHAR)
                .add(new ArrayType(INTEGER))
                .add(createType(INTEGER))
                .add(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of("nested_list"), new ArrayType(createType(VARCHAR))))
                        .add(new RowType.Field(Optional.of("nested_map"), createType(new ArrayType(VARCHAR))))
                        .build()))
                .build();
        assertEquals(types, expected);
    }

    @Test
    public void testUnsupportedUnionType()
    {
        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE)
                .convertAvroSchema(SchemaBuilder.record(RECORD_NAME).fields()
                        .name("union_col").type().unionOf().nullType().and().floatType().and().longType().endUnion().noDefault()
                        .endRecord()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Incompatible UNION type:");

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE)
                .convertAvroSchema(SchemaBuilder.record(RECORD_NAME).fields()
                        .name("union_col").type().unionOf().nullType().and().fixed("fixed").size(5).and().stringType().endUnion().noDefault()
                        .endRecord()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Incompatible UNION type:");

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE)
                .convertAvroSchema(SchemaBuilder.record(RECORD_NAME).fields()
                        .name("union_col").type().unionOf().nullType().and().booleanType().and().intType().endUnion().noDefault()
                        .endRecord()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Incompatible UNION type:");
    }

    @Test
    public void testSimpleSchema()
    {
        Schema schema = Schema.create(Schema.Type.LONG);
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(new TestingTypeManager(), IGNORE);
        List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
        assertEquals(getOnlyElement(types), BIGINT);
    }

    @Test
    public void testEmptyFieldStrategy()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("my_int").type().intType().noDefault()
                .name("my_record").type().record("nested_record")
                    .fields()
                    .endRecord()
                    .noDefault()
                .name("my_array").type().array().items().type("nested_record").noDefault()
                .name("my_map").type().map().values().type("nested_record").noDefault()
                .endRecord();

        List<Type> typesForIgnoreStrategy = ImmutableList.<Type>builder()
                .add(INTEGER)
                .build();

        assertEquals(new AvroSchemaConverter(new TestingTypeManager(), IGNORE).convertAvroSchema(schema), typesForIgnoreStrategy);

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), FAIL).convertAvroSchema(schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Struct type has no valid fields for schema: '%s'", SchemaBuilder.record("nested_record").fields().endRecord());

        List<Type> typesForAddDummyStrategy = ImmutableList.<Type>builder()
                .add(INTEGER)
                .add(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))
                        .build()))
                .add(new ArrayType(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))
                        .build())))
                .add(createType(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))
                        .build())))
                .build();

        assertEquals(new AvroSchemaConverter(new TestingTypeManager(), ADD_DUMMY).convertAvroSchema(schema), typesForAddDummyStrategy);
    }

    @Test
    public void testEmptyFieldStrategyForEmptySchema()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("my_record").type().record("nested_record")
                .fields()
                .endRecord()
                .noDefault()
                .name("my_array").type().array().items().type("nested_record").noDefault()
                .name("my_map").type().map().values().type("nested_record").noDefault()
                .endRecord();

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE).convertAvroSchema(schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Schema has no valid fields: '%s'", schema);

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), FAIL).convertAvroSchema(schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Struct type has no valid fields for schema: '%s'", SchemaBuilder.record("nested_record").fields().endRecord());

        List<Type> typesForAddDummyStrategy = ImmutableList.<Type>builder()
                .add(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))
                        .build()))
                .add(new ArrayType(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))
                        .build())))
                .add(createType(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))
                        .build())))
                .build();

        assertEquals(new AvroSchemaConverter(new TestingTypeManager(), ADD_DUMMY).convertAvroSchema(schema), typesForAddDummyStrategy);
    }

    private Type createType(Type valueType)
    {
        Type keyType = VARCHAR;
        return new MapType(keyType, valueType, TYPE_MANAGER.getTypeOperators());
    }
}
