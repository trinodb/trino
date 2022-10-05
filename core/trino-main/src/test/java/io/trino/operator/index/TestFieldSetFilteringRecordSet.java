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
package io.trino.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static org.testng.Assert.assertTrue;

public class TestFieldSetFilteringRecordSet
{
    @Test
    public void test()
    {
        ArrayType arrayOfBigintType = new ArrayType(BIGINT);
        FieldSetFilteringRecordSet fieldSetFilteringRecordSet = new FieldSetFilteringRecordSet(
                new TypeOperators(),
                new InMemoryRecordSet(
                        ImmutableList.of(BIGINT, BIGINT, TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MILLIS, arrayOfBigintType, arrayOfBigintType),
                        ImmutableList.of(
                                ImmutableList.of(
                                        100L,
                                        100L,
                                        // test same time in different time zone to make sure equal check was done properly
                                        packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                        packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                        // test structural type
                                        arrayBlockOf(BIGINT, 12, 34, 56),
                                        arrayBlockOf(BIGINT, 12, 34, 56)))),
                ImmutableList.of(ImmutableSet.of(0, 1), ImmutableSet.of(2, 3), ImmutableSet.of(4, 5)));
        RecordCursor recordCursor = fieldSetFilteringRecordSet.cursor();
        assertTrue(recordCursor.advanceNextPosition());
    }
}
