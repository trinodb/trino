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
package io.trino.hive.formats.line.grok;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.grok.exception.GrokException;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.hive.formats.FormatTestUtils.assertColumnValueEquals;
import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGrokFormat
{
    //Bug for bug tests
    @Test
    public void testBasicPatterns()
            throws IOException
    {
        String log = "hello 123";
        String inputFormat = "%{WORD:word} %{NUMBER:number}";

        assertLine(
                ImmutableList.of(
                        new Column("word", VARCHAR, 0),
                        new Column("number", VARCHAR, 1)),
                log,
                inputFormat,
                Optional.empty(),
                Arrays.asList("hello", "123"));
    }

    @Test
    public void testCustomPatterns()
            throws IOException
    {
        String log = "550e8400-e29b-41d4-a716-446655440000 CREATE";
        String inputFormat = "%{CUSTOM_UUID:id} %{WORD:action}";
        String inputGrokCustomPatterns = "CUSTOM_UUID [0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

        assertLine(
                ImmutableList.of(
                        new Column("id", VARCHAR, 0),
                        new Column("action", VARCHAR, 1)),
                log,
                inputFormat,
                Optional.of(inputGrokCustomPatterns),
                Arrays.asList("550e8400-e29b-41d4-a716-446655440000", "CREATE"));
    }

    @Test
    public void testCombinedPatterns()
            throws IOException
    {
        String log = "192.168.1.1 - john [27/Feb/2024:10:15:30 +0000] \"GET /api/v1/users HTTP/1.1\" 200 1234";
        String inputFormat = "%{IP:client} - %{USER:user} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:method} %{URIPATHPARAM:request} HTTP/%{NUMBER:httpversion}\" %{NUMBER:status} %{NUMBER:bytes}";

        assertLine(
                ImmutableList.of(
                        new Column("client", VARCHAR, 0),
                        new Column("user", VARCHAR, 1),
                        new Column("timestamp", VARCHAR, 2),
                        new Column("method", VARCHAR, 3),
                        new Column("request", VARCHAR, 4),
                        new Column("httpversion", VARCHAR, 5),
                        new Column("status", VARCHAR, 6),
                        new Column("bytes", VARCHAR, 7)),
                log,
                inputFormat,
                Optional.empty(),
                Arrays.asList("192.168.1.1", "john", "27/Feb/2024:10:15:30 +0000", "GET", "/api/v1/users", "1.1", "200", "1234"));
    }

    @Test
    public void testMissingInputFormat()
    {
        String log = "abc";
        List<Column> columns = ImmutableList.of(new Column("abc", VARCHAR, 0));

        assertError(columns, log, Optional.of(""), Optional.empty(), "Grok compilation failure: {pattern} should not be empty", Optional.of(GrokException.class)); // input.format is empty
        assertError(columns, log, Optional.of("     "), Optional.empty(), "Grok compilation failure: {pattern} should not be empty", Optional.of(GrokException.class)); // input.format is whitespace
        assertError(columns, log, Optional.empty(), Optional.empty(), "Schema does not have required 'input.format' property", Optional.empty()); // input.format is null
    }

    @Test
    public void testPatternDoesNotExist()
    {
        String log = "foo";
        List<Column> columns = ImmutableList.of(new Column("abc", VARCHAR, 0));

        assertError(columns, log, Optional.of("%{NONEXISTENT}"), Optional.empty(), "Grok compilation failure: Pattern NONEXISTENT is not defined.", Optional.of(GrokException.class));
    }

    // TODO: If pattern already exists, it just replaces the pattern, should we allow this to happen?

    // TODO: bug input.format = "%WORD}" with a missing opening brace should throw exception, but compiles fine right now

    @Test
    public void testNoMatches()
            throws IOException
    {
        String log = "foo bar";
        List<Column> columns = ImmutableList.of(new Column("abc", VARCHAR, 0), new Column("bar", VARCHAR, 1));
        String inputFormat = "%{NUMBER} %{WORD}";

        assertLine(
                columns,
                log,
                inputFormat,
                Optional.empty(),
                Arrays.asList(null, null));
    }

    @Test
    public void testUnderscoreInNamedGroups()
            throws IOException
    {
        String log = "abc";
        List<Column> columns = ImmutableList.of(new Column("abc", VARCHAR, 0));
        String inputFormat = "(?<name_underscore__>\\S+)";

        assertLine(
                columns,
                log,
                inputFormat,
                Optional.empty(),
                Arrays.asList("abc"));

        String complexLog = "192.168.1.1 GET [/api/users] \"Mozilla/5.0 Browser\" 200 \"Success Message\" \"Additional Details\" 1234ms";
        List<Column> complexColumns = ImmutableList.of(
                new Column("0", VARCHAR, 0),
                new Column("1", VARCHAR, 1),
                new Column("2", VARCHAR, 2),
                new Column("3", VARCHAR, 3),
                new Column("4", VARCHAR, 4),
                new Column("5", VARCHAR, 5),
                new Column("6", VARCHAR, 6),
                new Column("7", VARCHAR, 7));
        String complexInputFormat = "(?<na_me0>\\S+) (?<nam__e1>\\S+) \\[(?<__name2>([^\\]]*))\\] \"?(?<n_am_e3>([^\"]*))\"? (?<name4>\\S+) \"?(?<name5____>([^\"]*))\"? \"?(?<n_a_m_e_6>([^\"]*))\"? (?<__name__7__>\\S+)";

        assertLine(
                complexColumns,
                complexLog,
                complexInputFormat,
                Optional.empty(),
                Arrays.asList(
                        "192.168.1.1",
                        "GET",
                        "/api/users",
                        "Mozilla/5.0 Browser",
                        "200",
                        "Success Message",
                        "Additional Details",
                        "1234ms"));
    }

    @Test
    public void testExampleOneAthenaDocumentation()
            throws IOException
    {
        String log = "Feb 9 07:15:00 m4eastmail postfix/smtpd[19305]: B88C4120838: connect from unknown[192.168.55.4]";
        String inputFormat = "%{SYSLOGBASE} %{POSTFIX_QUEUEID:queue_id}: %{GREEDYDATA:syslog_message}";
        String inputGrokCustomPatterns = "POSTFIX_QUEUEID [0-9A-F]{7,12}";

        assertLine(
                ImmutableList.of(
                        new Column("SYSLOGBASE", VARCHAR, 0),
                        new Column("queue_id", VARCHAR, 1),
                        new Column("syslog_message", VARCHAR, 2)),
                log,
                inputFormat,
                Optional.of(inputGrokCustomPatterns),
                Arrays.asList(
                        "Feb 9 07:15:00", // BUG, should be => Feb 9 07:15:00 m4eastmail postfix/smtpd[19305]:
                        "m4eastmail", // BUG, should be => B88C4120838:
                        "postfix/smtpd")); // BUG, should be => connect from unknown[192.168.55.4]
    }

    @Test
    public void testExampleTwoAthenaDocumentation()
            throws IOException
    {
        String log = "2017-09-12 12:10:34,972 INFO - processType=AZ, processId=ABCDEFG614B6F5E49, status=RUN, " +
                "threadId=123:amqListenerContainerPool23P:AJ|ABCDE9614B6F5E49||2017-09-12T12:10:11.172-0700], " +
                "executionTime=7290, tenantId=12456, userId=123123f8535f8d76015374e7a1d87c3c, shard=testapp1, " +
                "jobId=12312345e5e7df0015e777fb2e03f3c, messageType=REAL_TIME_SYNC, " +
                "action=receive, hostname=1.abc.def.com";
        String inputFormat = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:loglevel} - processType=%{NO_COMMAS:processtype}, " +
                "processId=%{NO_COMMAS:processid}, status=%{NO_COMMAS:status}, threadId=%{NO_COMMAS:threadid}, " +
                "executionTime=%{POSINT:executiontime}, tenantId=%{POSINT:tenantid}, userId=%{NO_COMMAS:userid}, " +
                "shard=%{NO_COMMAS:shard}, jobId=%{NO_COMMAS:jobid}, messageType=%{NO_COMMAS:messagetype}, " +
                "action=%{C_ACTION:action}, hostname=%{HOST:hostname}";
        String inputGrokCustomPatterns = "C_ACTION (receive|send)\nNO_COMMAS ([^,]+)";

        assertLine(
                ImmutableList.of(
                        new Column("timestamp", VARCHAR, 0),
                        new Column("loglevel", VARCHAR, 1),
                        new Column("processtype", VARCHAR, 2),
                        new Column("processid", VARCHAR, 3),
                        new Column("status", VARCHAR, 4),
                        new Column("threadid", VARCHAR, 5),
                        new Column("executiontime", INTEGER, 6),
                        new Column("tenantid", INTEGER, 7),
                        new Column("userid", VARCHAR, 8),
                        new Column("shard", VARCHAR, 9),
                        new Column("jobid", VARCHAR, 10),
                        new Column("messagetype", VARCHAR, 11),
                        new Column("action", VARCHAR, 12),
                        new Column("hostname", VARCHAR, 13)),
                log,
                inputFormat,
                Optional.of(inputGrokCustomPatterns),
                Arrays.asList(
                        "2017-09-12 12:10:34,972",
                        "INFO",
                        "AZ",
                        "ABCDEFG614B6F5E49",
                        "RUN",
                        "123:amqListenerContainerPool23P:AJ|ABCDE9614B6F5E49||2017-09-12T12:10:11.172-0700]",
                        7290,
                        12456,
                        "123123f8535f8d76015374e7a1d87c3c",
                        "testapp1",
                        "12312345e5e7df0015e777fb2e03f3c",
                        "REAL_TIME_SYNC",
                        "receive",
                        "1.abc.def.com"));
    }

    @Test
    public void testExampleThreeAthenaDocumentation()
            throws IOException
    {
        String log = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be " +
                "amzn-s3-demo-bucket1 [06/Feb/2019:00:00:38 +0000] 192.0.2.3 " +
                "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING " +
                "- \"GET /amzn-s3-demo-bucket1?versioning HTTP/1.1\" 200 - 113 - 7 - \"-\" \"S3Console/0.4\" " +
                "- s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234= " +
                "SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader amzn-s3-demo-bucket1.s3.us-west-1.amazonaws.com " +
                "TLSV1.2 arn:aws:s3:us-west-1:123456789012:accesspoint/example-AP Yes";
        String inputFormat = "%{NOTSPACE:bucket_owner} %{NOTSPACE:bucket} \\[%{INSIDE_BRACKETS:time}\\] " +
                "%{NOTSPACE:remote_ip} %{NOTSPACE:requester} %{NOTSPACE:request_id} %{NOTSPACE:operation} " +
                "%{NOTSPACE:key} \"?%{INSIDE_QS:request_uri}\"? %{NOTSPACE:http_status} %{NOTSPACE:error_code} " +
                "%{NOTSPACE:bytes_sent} %{NOTSPACE:object_size} %{NOTSPACE:total_time} %{NOTSPACE:turnaround_time} " +
                "\"?%{INSIDE_QS:referrer}\"? \"?%{INSIDE_QS:user_agent}\"? %{NOTSPACE:version_id}";
        String inputGrokCustomPatterns = "INSIDE_QS ([^\"]*)\nINSIDE_BRACKETS ([^\\]]*)";

        assertLine(
                ImmutableList.of(
                        new Column("bucket_owner", VARCHAR, 0),
                        new Column("bucket", VARCHAR, 1),
                        new Column("time", VARCHAR, 2),
                        new Column("remote_ip", VARCHAR, 3),
                        new Column("requester", VARCHAR, 4),
                        new Column("request_id", VARCHAR, 5),
                        new Column("operation", VARCHAR, 6),
                        new Column("key", VARCHAR, 7),
                        new Column("request_uri", VARCHAR, 8),
                        new Column("http_status", VARCHAR, 9),
                        new Column("error_code", VARCHAR, 10),
                        new Column("bytes_sent", VARCHAR, 11),
                        new Column("object_size", VARCHAR, 12),
                        new Column("total_time", VARCHAR, 13),
                        new Column("turnaround_time", VARCHAR, 14),
                        new Column("referrer", VARCHAR, 15),
                        new Column("user_agent", VARCHAR, 16),
                        new Column("version_id", VARCHAR, 17)),
                log,
                inputFormat,
                Optional.of(inputGrokCustomPatterns),
                Arrays.asList(
                        "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be",
                        "amzn-s3-demo-bucket1",
                        "06/Feb/2019:00:00:38 +0000",
                        "192.0.2.3",
                        "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be",
                        "3E57427F3EXAMPLE",
                        "REST.GET.VERSIONING",
                        "-",
                        "GET /amzn-s3-demo-bucket1?versioning HTTP/1.1",
                        "200",
                        "-",
                        "113",
                        "-",
                        "7",
                        "-",
                        "-",
                        "S3Console/0.4",
                        "-"));
    }

    private static void assertError(List<Column> columns, String line, Optional<String> inputFormat, Optional<String> inputGrokCustomPatterns, String errorMessage, Optional<Class> expectedErrorClass)
    {
        ThrowableAssert.ThrowingCallable testAction = () -> readLine(columns, line, inputFormat, inputGrokCustomPatterns);

        AbstractThrowableAssert<?, ? extends Throwable> assertion = assertThatThrownBy(testAction).hasMessage(errorMessage);

        expectedErrorClass.ifPresent(assertion::hasRootCauseInstanceOf);
    }

    private static void assertLine(List<Column> columns, String line, String inputFormat, Optional<String> inputGrokCustomPatterns, List<Object> expectedValues)
            throws IOException
    {
        List<Object> actualValues = readLine(columns, line, Optional.of(inputFormat), inputGrokCustomPatterns);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
    }

    private static List<Object> readLine(List<Column> columns, String line, Optional<String> inputFormat, Optional<String> inputGrokCustomPatterns)
            throws IOException
    {
        LineDeserializer deserializer = new GrokDeserializerFactory().create(columns, createGrokProperties(inputFormat, inputGrokCustomPatterns));
        PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
        deserializer.deserialize(createLineBuffer(line), pageBuilder);
        return readTrinoValues(columns, pageBuilder.build(), 0);
    }

    private static Map<String, String> createGrokProperties(Optional<String> inputFormat, Optional<String> inputGrokCustomPatterns)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
        inputFormat.ifPresent(grokInputFormat -> schema.put(GrokDeserializerFactory.INPUT_FORMAT, grokInputFormat));
        inputGrokCustomPatterns.ifPresent(grokCustomPattern -> schema.put(GrokDeserializerFactory.INPUT_GROK_CUSTOM_PATTERNS, grokCustomPattern));
        return schema.buildOrThrow();
    }
}
