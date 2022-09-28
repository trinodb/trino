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
package io.trino.tests.product.utils;

import com.google.common.collect.ImmutableList;
import io.minio.messages.Event;
import io.minio.messages.EventType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.lang.String.join;

public final class MinioNotificationsAssertions
{
    private MinioNotificationsAssertions() {}

    public static void createNotificationsTable(String tableName)
    {
        onTrino().executeQuery(format("CREATE TABLE %s (" +
                "name varchar, " +
                "time timestamp, " +
                "bucket varchar, " +
                "object_key varchar, " +
                "request map<varchar, varchar>, " +
                "response map<varchar, varchar>, " +
                "user_agent varchar, " +
                "object_size int, " +
                "object_version varchar, " +
                "object_etag varchar, " +
                "sequencer varchar)", schemaTableName(tableName)));
    }

    public static void deleteNotificationsTable(String tableName)
    {
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", schemaTableName(tableName)));
    }

    private static String schemaTableName(String tableName)
    {
        return format("memory.default.\"%s\"", tableName);
    }

    public static void recordNotification(String tableName, Event event)
    {
        String insertQuery = format("INSERT INTO %s " +
                        "(name, time, request, response, user_agent, bucket, object_key, object_size, object_version, object_etag, sequencer) " +
                        "VALUES (%s, from_iso8601_timestamp(%s), %s, %s, %s, %s, url_decode(%s), %d, %s, %s, %s)",
                schemaTableName(tableName),
                quote(event.eventType().name()),
                quote(event.eventTime().toString()),
                toSqlLiteral(event.requestParameters()),
                toSqlLiteral(event.responseElements()),
                quote(event.userAgent()),
                quote(event.bucketName()),
                quote(event.objectName()),
                event.objectSize(),
                quote(event.objectVersionId()),
                quote(event.etag()),
                quote(event.sequencer()));

        onTrino().executeQuery(insertQuery);
    }

    private static String toSqlLiteral(Map<String, String> value)
    {
        if (value == null) {
            return "NULL";
        }

        ImmutableList.Builder<String> keys = ImmutableList.builder();
        ImmutableList.Builder<String> values = ImmutableList.builder();

        value.forEach((key, val) -> {
            keys.add(quote(key));
            values.add(quote(val));
        });

        return format("map(ARRAY[%s], ARRAY[%s])", join(", ", keys.build()), join(", ", values.build()));
    }

    private static String quote(String input)
    {
        if (input == null) {
            return "''";
        }

        return "'" + input.replaceAll("'", "''") + "'";
    }

    public static void assertNotificationsCount(String tableName, EventType eventType, String objectName, int expectedCalls)
    {
        List<List<?>> rows = onTrino().executeQuery(format("SELECT name, request, time " +
                        "FROM %s " +
                        "WHERE name = %s AND object_key = %s " +
                        "ORDER BY sequencer ASC", // sequencer specifies order of the notifications
                schemaTableName(tableName),
                quote(eventType.name()),
                quote(objectName))).rows();

        if (rows.size() != expectedCalls) {
            throw new AssertionError(format("Expected notification %s for '%s' %d time(s) but got %d: %s",
                    eventType,
                    objectName,
                    expectedCalls,
                    rows.size(),
                    formatEvents(rows)));
        }
    }

    private static String formatEvents(List<List<?>> calls)
    {
        return calls.stream()
                .map(row -> format("%s at %s with request: %s", row.get(0), row.get(1), row.get(2)))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
