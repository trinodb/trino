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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

@Isolated // changes the JVM default time zone
public class TestAbstractTrinoResultSetDefaultZone
{
    private static final ClientTypeSignature TIMESTAMP_0 = new ClientTypeSignature("timestamp", ImmutableList.of(ClientTypeSignatureParameter.ofLong(0)));

    @Test
    public void testDefaultZoneChange()
            throws SQLException
    {
        TimeZone originalZone = TimeZone.getDefault();
        try {
            assertDefaultZone("Asia/Kathmandu");
            assertDefaultZone("America/New_York");
            assertDefaultZone("Europe/Warsaw");
        }
        finally {
            TimeZone.setDefault(originalZone);
        }
    }

    private static void assertDefaultZone(String zone)
            throws SQLException
    {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(zone)));
        try (ResultSet resultSet = resultSet()) {
            assertThat(resultSet.next()).isTrue();
            Date date = Date.valueOf(LocalDate.of(2001, 5, 6));
            Time time = Time.valueOf(LocalTime.of(12, 34, 56));
            Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2001, 5, 6, 12, 34, 56));

            assertThat(resultSet.getDate(1)).isEqualTo(date);
            assertThat(resultSet.getObject(1)).isEqualTo(date);
            assertThat(resultSet.getTime(2)).isEqualTo(time);
            assertThat(resultSet.getObject(2)).isEqualTo(time);
            assertThat(resultSet.getTimestamp(3)).isEqualTo(timestamp);
            assertThat(resultSet.getObject(3)).isEqualTo(timestamp);
            assertThat((Object[]) resultSet.getArray(4).getArray()).containsExactly(timestamp);

            Date dateBefore1901 = Date.valueOf(LocalDate.of(1800, 5, 6));
            Timestamp timestampBefore1901 = Timestamp.valueOf(LocalDateTime.of(1800, 5, 6, 12, 34, 56));
            assertThat(resultSet.getDate(5)).isEqualTo(dateBefore1901);
            assertThat(resultSet.getObject(5)).isEqualTo(dateBefore1901);
            assertThat(resultSet.getTimestamp(6)).isEqualTo(timestampBefore1901);
            assertThat(resultSet.getObject(6)).isEqualTo(timestampBefore1901);
        }
    }

    @Test
    public void testDefaultZoneChangeAfterResultSetCreated()
            throws SQLException
    {
        TimeZone originalZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Kathmandu")));
            try (ResultSet resultSet = resultSet()) {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("America/New_York")));
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getObject(1, LocalDate.class)).isEqualTo(LocalDate.of(2001, 5, 6));
                assertThat(resultSet.getObject(3, LocalDateTime.class)).isEqualTo(LocalDateTime.of(2001, 5, 6, 12, 34, 56));
                assertThat(resultSet.getObject(5, LocalDate.class)).isEqualTo(LocalDate.of(1800, 5, 6));
                assertThat(resultSet.getObject(6, LocalDateTime.class)).isEqualTo(LocalDateTime.of(1800, 5, 6, 12, 34, 56));
            }
        }
        finally {
            TimeZone.setDefault(originalZone);
        }
    }

    private static ResultSet resultSet()
    {
        return new InMemoryTrinoResultSet(
                ImmutableList.of(
                        new Column("d", "date", new ClientTypeSignature("date")),
                        new Column("t", "time(0)", new ClientTypeSignature("time", ImmutableList.of(ClientTypeSignatureParameter.ofLong(0)))),
                        new Column("ts", "timestamp(0)", TIMESTAMP_0),
                        new Column("a", "array(timestamp(0))", new ClientTypeSignature("array", ImmutableList.of(ClientTypeSignatureParameter.ofType(TIMESTAMP_0)))),
                        new Column("d_1800", "date", new ClientTypeSignature("date")),
                        new Column("ts_1800", "timestamp(0)", TIMESTAMP_0)),
                ImmutableList.of(ImmutableList.of("2001-05-06", "12:34:56", "2001-05-06 12:34:56", ImmutableList.of("2001-05-06 12:34:56"), "1800-05-06", "1800-05-06 12:34:56")));
    }
}
