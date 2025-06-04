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
package io.trino.plugin.jdbc.logging;

import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFormatBasedRemoteQueryModifier
{
    @Test
    public void testCreatingCommentToAppendBasedOnFormatAndConnectorSession()
    {
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setTraceToken("trace_token")
                .setSource("source")
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("Query=$QUERY_ID Execution for user=$USER with source=$SOURCE ttoken=$TRACE_TOKEN");
        String modifiedQuery = modifier.apply(connectorSession, "SELECT * from USERS");

        assertThat(modifiedQuery)
                .isEqualTo("SELECT * from USERS /*Query=%s Execution for user=%s with source=%s ttoken=%s*/", connectorSession.getQueryId(), "Alice", "source", "trace_token");
    }

    @Test
    public void testCreatingCommentWithDuplicatedPredefinedValues()
    {
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setTraceToken("trace_token")
                .setSource("source")
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("$QUERY_ID, $QUERY_ID, $QUERY_ID, $QUERY_ID, $USER, $USER, $SOURCE, $SOURCE, $SOURCE, $TRACE_TOKEN, $TRACE_TOKEN");
        String modifiedQuery = modifier.apply(connectorSession, "SELECT * from USERS");

        assertThat(modifiedQuery)
                .isEqualTo("SELECT * from USERS /*%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s*/",
                        connectorSession.getQueryId(),
                        connectorSession.getQueryId(),
                        connectorSession.getQueryId(),
                        connectorSession.getQueryId(),
                        "Alice",
                        "Alice",
                        "source",
                        "source",
                        "source",
                        "trace_token",
                        "trace_token");
    }

    @Test
    public void testForSQLInjectionsByTraceToken()
    {
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setTraceToken("*/; DROP TABLE TABLE_A; /*")
                .setSource("source")
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("Query=$QUERY_ID Execution for user=$USER with source=$SOURCE ttoken=$TRACE_TOKEN");

        assertThatThrownBy(() -> modifier.apply(connectorSession, "SELECT * from USERS"))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Rendering metadata using 'query.comment-format' does not meet security criteria: Query=.* Execution for user=Alice with source=source ttoken=\\*/; DROP TABLE TABLE_A; /\\*");
    }

    @Test
    public void testForSQLInjectionsBySource()
    {
        testForSQLInjectionsBySource("*/; DROP TABLE TABLE_A; /*");
        testForSQLInjectionsBySource("Prefix */; DROP TABLE TABLE_A; /*");
        testForSQLInjectionsBySource(
                """


                Multiline */; DROP TABLE TABLE_A; /*\
                """);
    }

    @Test
    public void testFormatQueryModifierWithUser()
    {
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .setSource("$invalid@value")
                .setTraceToken("#invalid&value")
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("user=$USER");

        assertThat(modifier.apply(connectorSession, "SELECT * FROM USERS"))
                .isEqualTo("SELECT * FROM USERS /*user=Alice*/");
    }

    @Test
    public void testFormatQueryModifierWithSource()
    {
        String validValue = "valid-value";
        String invalidValue = "$invalid@value";

        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .setSource(validValue)
                .setTraceToken(invalidValue)
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("source=$SOURCE");

        assertThat(modifier.apply(connectorSession, "SELECT * FROM USERS"))
                .isEqualTo("SELECT * FROM USERS /*source=valid-value*/");
    }

    @Test
    public void testFormatQueryModifierWithTraceToken()
    {
        String validValue = "valid-value";
        String invalidValue = "$invalid@value";

        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .setSource(invalidValue)
                .setTraceToken(validValue)
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("ttoken=$TRACE_TOKEN");

        assertThat(modifier.apply(connectorSession, "SELECT * FROM USERS"))
                .isEqualTo("SELECT * FROM USERS /*ttoken=valid-value*/");
    }

    @Test
    public void testFormatWithValidValues()
    {
        testFormatWithValidValues("trino");
        testFormatWithValidValues("123");
        testFormatWithValidValues("1t2r3i4n0");
        testFormatWithValidValues("trino-cli");
        testFormatWithValidValues("trino_cli");
        testFormatWithValidValues("trino-cli_123");
        testFormatWithValidValues("123_trino-cli");
        testFormatWithValidValues("123-trino_cli");
        testFormatWithValidValues("-trino-cli");
        testFormatWithValidValues("_trino_cli");
    }

    private void testFormatWithValidValues(String value)
    {
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .setSource(value)
                .setTraceToken(value)
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("source=$SOURCE ttoken=$TRACE_TOKEN");

        String modifiedQuery = modifier.apply(connectorSession, "SELECT * FROM USERS");

        assertThat(modifiedQuery)
                .isEqualTo("SELECT * FROM USERS /*source=%1$s ttoken=%1$s*/".formatted(value));
    }

    private void testForSQLInjectionsBySource(String sqlInjection)
    {
        TestingConnectorSession connectorSession = TestingConnectorSession.builder()
                .setTraceToken("trace_token")
                .setSource(sqlInjection)
                .setIdentity(ConnectorIdentity.ofUser("Alice"))
                .build();

        FormatBasedRemoteQueryModifier modifier = createRemoteQueryModifier("Query=$QUERY_ID Execution for user=$USER with source=$SOURCE ttoken=$TRACE_TOKEN");

        assertThatThrownBy(() -> modifier.apply(connectorSession, "SELECT * from USERS"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Rendering metadata using 'query.comment-format' does not meet security criteria: Query=");
    }

    private static FormatBasedRemoteQueryModifier createRemoteQueryModifier(String commentFormat)
    {
        return new FormatBasedRemoteQueryModifier(new FormatBasedRemoteQueryModifierConfig().setFormat(commentFormat));
    }
}
