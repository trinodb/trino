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
package io.trino.delta;

import io.trino.spi.TrinoException;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Delta table name. Supported table name formats:
 * <ul>
 *     <li>table name. E.g. SELECT * FROM delta.db.sales</li>
 *      <li>path to the Delta table location. E.g. SELECT * FROM "s3://bucket/path/delta-table"</li>
 * </ul>
 * <p>
 * Table name can have a suffix to indicate reading a particular snapshot of the table
 * <ul>
 *     <li>Snapshot version. E.g. SELECT * FROM delta.db."sales@v123" -- to read 123 snapshot version </li>
 *     <li>Latest snapshot as of given timestamp. E.g. SELECT * FROM delta.db."sales@t2021-11-18 10:30:00"
 *          -- to read latest snapshot on or before 2021-11-18 10:30:00</li>
 * </ul>
 */
public class DeltaTableName
{
    private static final Pattern TABLE_PATTERN = Pattern.compile(
            "(?<table>[^@]+)" + /* matches table name that doesn't contain `@` character */
                    "(?:@v(?<snapshotId>[0-9]+))?" +
                    "(?:@t(?<timestamp>[0-9.\\-: ]+))?");

    private static final DateTimeFormatter TIMESTAMP_PARSER =
            new DateTimeFormatterBuilder().appendPattern("yyyy[-MM[-dd[ HH[:mm[:ss]]]]]")
                    .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                    .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter()
                    .withZone(ZoneId.of("UTC"));

    private final String tableNameOrPath;
    private final Optional<Long> snapshotId;
    private final Optional<Long> snapshotAsOfTimestamp;

    public DeltaTableName(String tableNameOrPath, Optional<Long> snapshotId, Optional<Long> snapshotAsOfTimestamp)
    {
        this.tableNameOrPath = requireNonNull(tableNameOrPath, "tableNameOrPath is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.snapshotAsOfTimestamp = requireNonNull(snapshotAsOfTimestamp, "snapshotAsOfTimestamp is null");
    }

    public String getTableNameOrPath()
    {
        return tableNameOrPath;
    }

    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    public Optional<Long> getSnapshotAsOfTimestamp()
    {
        return snapshotAsOfTimestamp;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder()
                .append("Table[" + tableNameOrPath + "]");
        snapshotId.map(id -> builder.append("@v" + id));
        snapshotAsOfTimestamp.map(ts -> builder.append("@t" + new Timestamp(ts)));
        return builder.toString();
    }

    public static DeltaTableName from(String tableName)
    {
        Matcher match = TABLE_PATTERN.matcher(tableName);
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Delta table name: " + tableName +
                    ", Expected table name form 'tableName[@v<snapshotId>][@t<snapshotAsOfTimestamp>]'. " +
                    "The table can have either a particular snapshot identifier or a timestamp of the snapshot. " +
                    "If timestamp is given the latest snapshot of the table that was generated at or " +
                    "before the given timestamp is read");
        }

        String tableNameOrPath = match.group("table");
        String snapshotValue = match.group("snapshotId");
        Optional<Long> snapshot = Optional.empty();
        if (snapshotValue != null) {
            snapshot = Optional.of(Long.parseLong(snapshotValue));
        }

        Optional<Long> timestampMillisUtc = Optional.empty();
        String timestampValue = match.group("timestamp");
        if (timestampValue != null) {
            try {
                timestampMillisUtc = Optional.of(
                        LocalDateTime.from(TIMESTAMP_PARSER.parse(timestampValue))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli());
            }
            catch (IllegalArgumentException | DateTimeParseException ex) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        format("Invalid Delta table name: %s, given snapshot timestamp (%s) format is not valid. " +
                                "Expected timestamp format 'YYYY-MM-DD HH:mm:ss'", tableName, timestampValue),
                        ex);
            }
        }

        if (snapshot.isPresent() && timestampMillisUtc.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Delta table name: " + tableName +
                    ", Table suffix contains both snapshot id and timestamp of snapshot to read. " +
                    "Only one of them is supported.");
        }

        return new DeltaTableName(tableNameOrPath, snapshot, timestampMillisUtc);
    }
}
