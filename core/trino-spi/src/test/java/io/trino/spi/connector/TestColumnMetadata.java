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
package io.trino.spi.connector;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestColumnMetadata
{
    @Test
    public void testBuilderFrom()
    {
        ColumnMetadata originColumnMetadata = new ColumnMetadata(
                "test_column",
                INTEGER,
                Optional.empty(),
                Optional.of("1"),
                false,
                Optional.of("test_comment"),
                Optional.of("test_extra_info"),
                false,
                ImmutableMap.of("test_key", "test_value"));

        ColumnMetadata buildColumnMetadata = ColumnMetadata.builderFrom(originColumnMetadata).build();

        assertThat(buildColumnMetadata).isEqualTo(originColumnMetadata);
        assertThat(buildColumnMetadata.getDefaultValue()).isEqualTo(originColumnMetadata.getDefaultValue());
        assertThat(buildColumnMetadata.getProperties()).isEqualTo(originColumnMetadata.getProperties());
    }

    @Test
    public void testTimeZone()
    {
        ColumnMetadata columnMetadata = ColumnMetadata.builder()
                .setName("test_column")
                .setType(TIMESTAMP_TZ_MILLIS)
                .setTimeZone(Optional.of(getTimeZoneKey("Europe/Warsaw")))
                .build();
        assertThat(ColumnMetadata.builderFrom(columnMetadata).build().getTimeZone()).contains(getTimeZoneKey("Europe/Warsaw"));
        assertThat(ColumnMetadata.builderFrom(columnMetadata).build()).isEqualTo(columnMetadata);
        assertThat(ColumnMetadata.builderFrom(columnMetadata).setTimeZone(Optional.of(UTC_KEY)).build()).isNotEqualTo(columnMetadata);
        assertThat(ColumnMetadata.builderFrom(columnMetadata).setTimeZone(Optional.empty()).build()).isNotEqualTo(columnMetadata);

        assertThatThrownBy(() -> ColumnMetadata.builder()
                .setName("test_column")
                .setType(TIMESTAMP_MILLIS)
                .setTimeZone(Optional.of(UTC_KEY))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("timeZone is set for column test_column of type timestamp(3), which contains no timestamp with time zone");

        RowType nested = RowType.anonymousRow(INTEGER, new ArrayType(TIMESTAMP_TZ_MILLIS));
        assertThat(ColumnMetadata.builder()
                .setName("test_column")
                .setType(nested)
                .setTimeZone(Optional.of(UTC_KEY))
                .build()
                .getTimeZone())
                .contains(UTC_KEY);
    }
}
