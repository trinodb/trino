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
package io.trino.plugin.hive.aws.athena;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_DIGITS;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIntegerProjectionFactory
{
    @Test
    void testIsSupported()
    {
        new IntegerProjection("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("1", "3")));
        new IntegerProjection("test", INTEGER, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("1", "3")));
        new IntegerProjection("test", BIGINT, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("1", "3")));
        assertThatThrownBy(() -> new IntegerProjection("test", DATE, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("1", "3"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Unsupported column type: date");
    }

    @Test
    void testCreateBasic()
    {
        Projection projection = new IntegerProjection("test", INTEGER, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("1", "3")));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("1", "2", "3");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(INTEGER)))).containsExactly("1", "2", "3");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(INTEGER)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(INTEGER, 2L)))).containsExactly("2");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(INTEGER, 7L)))).isEmpty();

        assertThatThrownBy(() -> new IntegerProjection("test", INTEGER, ImmutableMap.of("ignored", ImmutableList.of("1", "3"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Missing required property: 'partition_projection_range'");

        assertThatThrownBy(() -> new IntegerProjection("test", INTEGER, ImmutableMap.of(COLUMN_PROJECTION_RANGE, "invalid")))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void testInterval()
    {
        Projection projection = new IntegerProjection("test", INTEGER, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("10", "30"), COLUMN_PROJECTION_INTERVAL, 10));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("10", "20", "30");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(INTEGER)))).containsExactly("10", "20", "30");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(INTEGER)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(INTEGER, 20L)))).containsExactly("20");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(INTEGER, 70L)))).isEmpty();
    }

    @Test
    void testCreateDigits()
    {
        Projection projection = new IntegerProjection("test", INTEGER, ImmutableMap.of(COLUMN_PROJECTION_RANGE, ImmutableList.of("1", "3"), COLUMN_PROJECTION_DIGITS, 3));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("001", "002", "003");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(INTEGER)))).containsExactly("001", "002", "003");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(INTEGER)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(INTEGER, 2L)))).containsExactly("002");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(INTEGER, 7L)))).isEmpty();
    }
}
