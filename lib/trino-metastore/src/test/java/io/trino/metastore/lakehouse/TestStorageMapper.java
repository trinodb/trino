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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.FieldSchema;
import com.google.cloud.biglake.hive.v1.SerdeInfo;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.cloud.biglake.hive.v1.StorageDescriptor.Order;
import com.google.cloud.biglake.hive.v1.StorageDescriptor.SkewedInfo;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Storage;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.MetastoreErrorCode.HIVE_INVALID_METADATA;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PARQUET_INPUT_FORMAT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PARQUET_OUTPUT_FORMAT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PARQUET_SERDE;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestStorageMapper
{
    private static final String NAME = "sales.orders";

    @Test
    void mapsFormatLocationAndSerdeParameters()
    {
        StorageDescriptor source = descriptor()
                .setLocationUri("gs://warehouse/orders")
                .setSerdeInfo(SerdeInfo.newBuilder()
                        .setSerializationLib(PARQUET_SERDE)
                        .putAllParameters(Map.of("serialization.format", "1")))
                .build();

        Storage mapped = StorageMapper.toTrino(source, NAME);

        assertThat(mapped.getStorageFormat().getSerde()).isEqualTo(PARQUET_SERDE);
        assertThat(mapped.getStorageFormat().getInputFormat()).isEqualTo(PARQUET_INPUT_FORMAT);
        assertThat(mapped.getStorageFormat().getOutputFormat()).isEqualTo(PARQUET_OUTPUT_FORMAT);
        assertThat(mapped.getOptionalLocation()).contains("gs://warehouse/orders");
        assertThat(mapped.getSerdeParameters()).containsExactlyEntriesOf(Map.of("serialization.format", "1"));
    }

    @Test
    void reportsAnAbsentLocationAsEmptyRatherThanBlank()
    {
        Storage mapped = StorageMapper.toTrino(descriptor().build(), NAME);

        assertThat(mapped.getOptionalLocation()).isEmpty();
    }

    @Test
    void reportsAnAbsentFormatAsNullRatherThanBlank()
    {
        StorageDescriptor source = StorageDescriptor.newBuilder()
                .setSerdeInfo(SerdeInfo.getDefaultInstance())
                .build();

        Storage mapped = StorageMapper.toTrino(source, NAME);

        assertThat(mapped.getStorageFormat().getSerDeNullable()).isNull();
        assertThat(mapped.getStorageFormat().getInputFormatNullable()).isNull();
        assertThat(mapped.getStorageFormat().getOutputFormatNullable()).isNull();
    }

    @Test
    void rejectsADescriptorWithoutSerdeInfo()
    {
        assertThatThrownBy(() -> StorageMapper.toTrino(StorageDescriptor.getDefaultInstance(), NAME))
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, HIVE_INVALID_METADATA))
                .hasMessageContaining("SerDe")
                .hasMessageContaining(NAME);
    }

    @Test
    void marksAStorageAsSkewedOnlyWhenSkewedColumnsAreListed()
    {
        Storage plain = StorageMapper.toTrino(descriptor().build(), NAME);
        Storage skewed = StorageMapper.toTrino(
                descriptor()
                        .setSkewedInfo(SkewedInfo.newBuilder().addSkewedColNames("region"))
                        .build(),
                NAME);

        assertThat(plain.isSkewed()).isFalse();
        assertThat(skewed.isSkewed()).isTrue();
    }

    @Test
    void mapsColumnNamesTypesAndComments()
    {
        List<Column> mapped = StorageMapper.toTrinoColumns(
                List.of(
                        field("id", "int"),
                        FieldSchema.newBuilder().setName("name").setType("string").setComment("customer name").build()),
                NAME);

        assertThat(mapped).extracting(Column::getName).containsExactly("id", "name");
        assertThat(mapped).extracting(Column::getType).containsExactly(HIVE_INT, HIVE_STRING);
        assertThat(mapped).extracting(Column::getComment).containsExactly(Optional.empty(), Optional.of("customer name"));
    }

    @Test
    void readsATypeNameInAnyCase()
    {
        assertThat(StorageMapper.toTrinoColumns(List.of(field("id", "INT")), NAME))
                .extracting(Column::getType)
                .containsExactly(HIVE_INT);
    }

    @Test
    void mapsNestedAndParameterizedTypes()
    {
        List<String> types = List.of("array<struct<a:int,b:string>>", "map<string,bigint>", "decimal(10,2)", "varchar(20)");

        List<Column> mapped = StorageMapper.toTrinoColumns(
                types.stream().map(type -> field("c", type)).toList(),
                NAME);

        assertThat(mapped).extracting(column -> column.getType().toString()).isEqualTo(types);
    }

    @Test
    void rejectsAColumnTypeThatTrinoCannotParse()
    {
        assertThatThrownBy(() -> StorageMapper.toTrinoColumns(List.of(field("id", "not a type")), NAME))
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, HIVE_INVALID_METADATA))
                .hasMessageContaining("id")
                .hasMessageContaining(NAME)
                .hasMessageContaining("not a type");
    }

    @Test
    void mapsBucketingWithItsSortColumns()
    {
        StorageDescriptor source = descriptor()
                .setNumBuckets(8)
                .addBucketCols("id")
                .addSortCols(Order.newBuilder().setCol("id").setOrder(1))
                .addSortCols(Order.newBuilder().setCol("name").setOrder(0))
                .build();

        assertThat(StorageMapper.toTrino(source, NAME).getBucketProperty())
                .contains(new HiveBucketProperty(
                        List.of("id"),
                        8,
                        List.of(
                                new SortingColumn("id", SortingColumn.Order.ASCENDING),
                                new SortingColumn("name", SortingColumn.Order.DESCENDING))));
    }

    @Test
    void treatsAZeroOrNegativeBucketCountAsNotBucketed()
    {
        // Hive reports -1 for a table that is not bucketed
        for (int buckets : List.of(0, -1)) {
            StorageDescriptor source = descriptor().setNumBuckets(buckets).addBucketCols("id").build();

            assertThat(StorageMapper.toTrino(source, NAME).getBucketProperty()).isEmpty();
        }
    }

    @Test
    void rejectsABucketCountWithoutBucketColumns()
    {
        StorageDescriptor source = descriptor().setNumBuckets(8).build();

        assertThatThrownBy(() -> StorageMapper.toTrino(source, NAME))
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, HIVE_INVALID_METADATA))
                .hasMessageContaining("bucket")
                .hasMessageContaining(NAME);
    }

    @Test
    void rejectsAnUnknownSortOrder()
    {
        StorageDescriptor source = descriptor()
                .setNumBuckets(8)
                .addBucketCols("id")
                .addSortCols(Order.newBuilder().setCol("id").setOrder(2))
                .build();

        assertThatThrownBy(() -> StorageMapper.toTrino(source, NAME))
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, HIVE_INVALID_METADATA))
                .hasMessageContaining("sorting order")
                .hasMessageContaining(NAME);
    }

    private static StorageDescriptor.Builder descriptor()
    {
        return StorageDescriptor.newBuilder()
                .setInputFormat(PARQUET_INPUT_FORMAT)
                .setOutputFormat(PARQUET_OUTPUT_FORMAT)
                .setSerdeInfo(SerdeInfo.newBuilder().setSerializationLib(PARQUET_SERDE));
    }

    private static void assertErrorCode(TrinoException thrown, ErrorCodeSupplier expected)
    {
        assertThat(thrown.getErrorCode()).isEqualTo(expected.toErrorCode());
    }
}
