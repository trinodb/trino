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
package io.trino.plugin.ai.functions;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.assertj.AttributesAssert;
import io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions;
import io.specto.hoverfly.junit.core.Hoverfly;
import io.specto.hoverfly.junit5.TrinoHoverflyExtension;
import io.trino.sql.SqlPath;
import io.trino.sql.query.QueryAssertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@ExtendWith(TrinoHoverflyExtension.class)
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public abstract class AbstractTestAiFunctions
{
    protected static final InstanceOfAssertFactory<Attributes, AttributesAssert> ATTRIBUTES =
            new InstanceOfAssertFactory<>(Attributes.class, OpenTelemetryAssertions::assertThat);

    protected QueryAssertions assertions;

    protected abstract Map<String, String> getProperties(HostAndPort hoverflyAddress);

    @BeforeAll
    public void init(Hoverfly hoverfly)
    {
        assertions = new QueryAssertions(testSessionBuilder()
                .setPath(SqlPath.buildPath("ai.ai", Optional.empty()))
                .build());
        assertions.addPlugin(new AiPlugin());
        assertions.getQueryRunner().createCatalog("ai", "ai", getProperties(
                HostAndPort.fromParts("localhost", hoverfly.getHoverflyConfig().getProxyPort())));
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testShowFunctions()
    {
        assertThat(assertions.query("SHOW FUNCTIONS LIKE 'ai$_%' ESCAPE '$'"))
                .skippingTypesCheck()
                .matches("""
                         VALUES
                         ('ai_analyze_sentiment', 'varchar', 'varchar', 'scalar', false, 'Perform sentiment analysis on text'),
                         ('ai_classify', 'varchar', 'varchar, array(varchar)', 'scalar', false, 'Classify text with the provided labels'),
                         ('ai_extract', 'map(varchar,varchar)', 'varchar, array(varchar)', 'scalar', false, 'Extract values for the provided labels from text'),
                         ('ai_fix_grammar', 'varchar', 'varchar', 'scalar', false, 'Correct grammatical errors in text'),
                         ('ai_gen', 'varchar', 'varchar', 'scalar', false, 'Generate text based on a prompt'),
                         ('ai_mask', 'varchar', 'varchar, array(varchar)', 'scalar', false, 'Mask values for the provided labels in text'),
                         ('ai_translate', 'varchar', 'varchar, varchar', 'scalar', false, 'Translate text to the specified language')
                         """);
    }

    @Test
    public void testAnalyzeSentiment()
    {
        assertThat(assertions.function("ai_analyze_sentiment", "'I love Trino'"))
                .hasType(VARCHAR)
                .isEqualTo("positive");

        assertThat(assertions.function("ai_analyze_sentiment", "'I am sad'"))
                .hasType(VARCHAR)
                .isEqualTo("negative");

        assertThat(assertions.function("ai_analyze_sentiment", "'The earth is round'"))
                .hasType(VARCHAR)
                .isEqualTo("neutral");

        assertThat(assertions.function("ai_analyze_sentiment", "'The food tasted good but was too salty'"))
                .hasType(VARCHAR)
                .isEqualTo("mixed");
    }

    @Test
    public void testClassifyForSentiment()
    {
        assertThat(assertions.function("ai_classify", "'I love Trino'", "ARRAY['positive', 'negative', 'neutral', 'mixed']"))
                .hasType(VARCHAR)
                .isEqualTo("positive");

        assertThat(assertions.function("ai_classify", "'I am sad'", "ARRAY['positive', 'negative', 'neutral', 'mixed']"))
                .hasType(VARCHAR)
                .isEqualTo("negative");

        assertThat(assertions.function("ai_classify", "'The sky is blue'", "ARRAY['positive', 'negative', 'neutral', 'mixed']"))
                .hasType(VARCHAR)
                .isEqualTo("neutral");

        assertThat(assertions.function("ai_classify", "'The food tasted good but was too salty'", "ARRAY['positive', 'negative', 'neutral', 'mixed']"))
                .hasType(VARCHAR)
                .isEqualTo("mixed");
    }

    @Test
    public void testClassifyForUrgency()
    {
        assertThat(assertions.function("ai_classify", "'The deadline is approaching'", "ARRAY['urgent', 'not urgent']"))
                .hasType(VARCHAR)
                .isEqualTo("urgent");

        assertThat(assertions.function("ai_classify", "'The deadline is far away'", "ARRAY['urgent', 'not urgent']"))
                .hasType(VARCHAR)
                .isEqualTo("not urgent");
    }

    @Test
    public void testClassifyForSpam()
    {
        assertThat(assertions.function("ai_classify", "'Buy now!'", "ARRAY['spam', 'not spam']"))
                .hasType(VARCHAR)
                .isEqualTo("spam");

        assertThat(assertions.function("ai_classify", "'Hello, how are you?'", "ARRAY['spam', 'not spam']"))
                .hasType(VARCHAR)
                .isEqualTo("not spam");
    }

    @Test
    public void testClassifyForCategory()
    {
        assertThat(assertions.function("ai_classify", "'Buy a new dress'", "ARRAY['clothing', 'shoes', 'accessories', 'furniture']"))
                .hasType(VARCHAR)
                .isEqualTo("clothing");

        assertThat(assertions.function("ai_classify", "'Buy a new chair'", "ARRAY['clothing', 'shoes', 'accessories', 'furniture']"))
                .hasType(VARCHAR)
                .isEqualTo("furniture");

        assertThat(assertions.function("ai_classify", "'Buy a new bag'", "ARRAY['clothing', 'shoes', 'accessories', 'furniture']"))
                .hasType(VARCHAR)
                .isEqualTo("accessories");
    }

    @Test
    public void testExtractWithOneMatch()
    {
        assertThat(assertions.function("ai_extract", "'The deadline is tomorrow'", "ARRAY['date']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("date", "tomorrow"));

        assertThat(assertions.function("ai_extract", "'The deadline is 2022-12-31'", "ARRAY['date']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("date", "2022-12-31"));

        assertThat(assertions.function("ai_extract", "'The deadline is 2022-12-31 and the budget is $1000'", "ARRAY['date', 'money']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("date", "2022-12-31", "money", "$1000"));

        assertThat(assertions.function("ai_extract", "'John Doe lives in New York and works for Acme Corp.'", "ARRAY['person', 'location', 'organization']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("person", "John Doe", "location", "New York", "organization", "Acme Corp"));

        assertThat(assertions.function("ai_extract", "'Send an email to jane.doe@example.com about the meeting at 10am.'", "ARRAY['email', 'time']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("email", "jane.doe@example.com", "time", "10am"));
    }

    @Test
    public void testExtractWithMultipleMatches()
    {
        assertThat(assertions.function("ai_extract", "'The deadline is 2022-12-31 and the meeting is on 2022-12-30'", "ARRAY['date']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("date", "2022-12-31"));
    }

    @Test
    public void testExtractWithNoMatches()
    {
        assertThat(assertions.function("ai_extract", "'The deadline is tomorrow'", "ARRAY['money']"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testFixGrammar()
    {
        assertThat(assertions.function("ai_fix_grammar", "'I are happy. What you doing?'"))
                .hasType(VARCHAR)
                .isEqualTo("I am happy. What are you doing?");

        assertThat(assertions.function("ai_fix_grammar", "'The sky ain''t blue.'"))
                .hasType(VARCHAR)
                .isEqualTo("The sky isn't blue.");

        assertThat(assertions.function("ai_fix_grammar", "'This sentence have some mistake'"))
                .hasType(VARCHAR)
                .isEqualTo("This sentence has some mistakes.");

        assertThat(assertions.function("ai_fix_grammar", "'She dont know what to did.'"))
                .hasType(VARCHAR)
                .isEqualTo("She doesn't know what to do.");

        assertThat(assertions.function("ai_fix_grammar", "'He go to school every days.'"))
                .hasType(VARCHAR)
                .isEqualTo("He goes to school every day.");
    }

    @Test
    public void testGen()
    {
        assertThat(assertions.function("ai_gen", "'What is the capital of France?'"))
                .hasType(VARCHAR)
                .isEqualTo("The capital of France is Paris.");
    }

    @Test
    public void testMask()
    {
        assertThat(assertions.function("ai_mask", "'You can message me at jsmith@example.com'", "ARRAY['email']"))
                .hasType(VARCHAR)
                .isEqualTo("You can message me at [MASKED]");

        assertThat(assertions.function("ai_mask", "'Contact me at 555-1234 or visit us at 123 Main St.'", "ARRAY['phone', 'address']"))
                .hasType(VARCHAR)
                .isEqualTo("Contact me at [MASKED] or visit us at [MASKED].");
    }

    @Test
    public void testTranslate()
    {
        assertThat(assertions.function("ai_translate", "'I like coffee'", "'fr'"))
                .hasType(VARCHAR)
                .isEqualTo("J'aime le café");

        assertThat(assertions.function("ai_translate", "'I like coffee'", "'es'"))
                .hasType(VARCHAR)
                .isEqualTo("Me gusta el café");

        assertThat(assertions.function("ai_translate", "'I like coffee'", "'zh-CN'"))
                .hasType(VARCHAR)
                .isEqualTo("我喜欢咖啡");

        assertThat(assertions.function("ai_translate", "'I like coffee'", "'zh-TW'"))
                .hasType(VARCHAR)
                .isEqualTo("我喜歡咖啡");
    }
}
