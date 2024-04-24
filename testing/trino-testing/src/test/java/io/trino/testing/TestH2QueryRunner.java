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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestH2QueryRunner
{
    @Test
    public void testDateToTimestampCoercion()
    {
        try (H2QueryRunner h2QueryRunner = new H2QueryRunner()) {
            // allow running tests with a connector that supports TIMESTAMP but not DATE

            // ordinary date
            MaterializedResult rows = h2QueryRunner.execute(TEST_SESSION, "SELECT DATE '2018-01-13'", ImmutableList.of(TIMESTAMP_MILLIS));
            assertThat(rows.getOnlyValue()).isEqualTo(LocalDate.of(2018, 1, 13).atStartOfDay());

            // date, which midnight was skipped in JVM zone
            LocalDate forwardOffsetChangeAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
            checkState(ZoneId.systemDefault().getRules().getValidOffsets(forwardOffsetChangeAtMidnightInJvmZone.atStartOfDay()).isEmpty(), "This test assumes certain JVM time zone");
            rows = h2QueryRunner.execute(TEST_SESSION, DateTimeFormatter.ofPattern("'SELECT DATE '''uuuu-MM-dd''").format(forwardOffsetChangeAtMidnightInJvmZone), ImmutableList.of(TIMESTAMP_MILLIS));
            assertThat(rows.getOnlyValue()).isEqualTo(forwardOffsetChangeAtMidnightInJvmZone.atStartOfDay());
        }
    }
}
