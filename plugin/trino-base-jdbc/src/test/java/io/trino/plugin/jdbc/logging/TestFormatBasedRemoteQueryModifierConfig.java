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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFormatBasedRemoteQueryModifierConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FormatBasedRemoteQueryModifierConfig.class).setFormat(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().put("query.comment-format", "format").buildOrThrow();

        FormatBasedRemoteQueryModifierConfig expected = new FormatBasedRemoteQueryModifierConfig().setFormat("format");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidFormatValue()
    {
        assertThat(configWithFormat("*").isFormatValid()).isFalse();
        assertThat(configWithFormat("(").isFormatValid()).isFalse();
        assertThat(configWithFormat(")").isFormatValid()).isFalse();
        assertThat(configWithFormat("[").isFormatValid()).isFalse();
        assertThat(configWithFormat("]").isFormatValid()).isFalse();
        assertThat(configWithFormat("{").isFormatValid()).isFalse();
        assertThat(configWithFormat("}").isFormatValid()).isFalse();
        assertThat(configWithFormat("&").isFormatValid()).isFalse();
        assertThat(configWithFormat("@").isFormatValid()).isFalse();
        assertThat(configWithFormat("!").isFormatValid()).isFalse();
        assertThat(configWithFormat("#").isFormatValid()).isFalse();
        assertThat(configWithFormat("%").isFormatValid()).isFalse();
        assertThat(configWithFormat("^").isFormatValid()).isFalse();
        assertThat(configWithFormat("$").isFormatValid()).isFalse();
        assertThat(configWithFormat("\\").isFormatValid()).isFalse();
        assertThat(configWithFormat("/").isFormatValid()).isFalse();
        assertThat(configWithFormat("?").isFormatValid()).isFalse();
        assertThat(configWithFormat(">").isFormatValid()).isFalse();
        assertThat(configWithFormat("<").isFormatValid()).isFalse();
        assertThat(configWithFormat(";").isFormatValid()).isFalse();
        assertThat(configWithFormat("\"").isFormatValid()).isFalse();
        assertThat(configWithFormat(":").isFormatValid()).isFalse();
        assertThat(configWithFormat("|").isFormatValid()).isFalse();
    }

    @Test
    public void testValidFormatWithPredefinedValues()
    {
        assertThat(new FormatBasedRemoteQueryModifierConfig().setFormat("$QUERY_ID $USER $SOURCE $TRACE_TOKEN").isFormatValid()).isTrue();
    }

    @Test
    public void testValidFormatWithDuplicatedPredefinedValues()
    {
        assertThat(new FormatBasedRemoteQueryModifierConfig().setFormat("$QUERY_ID $QUERY_ID $USER $USER $SOURCE $SOURCE $TRACE_TOKEN $TRACE_TOKEN").isFormatValid()).isTrue();
    }

    private FormatBasedRemoteQueryModifierConfig configWithFormat(String format)
    {
        return new FormatBasedRemoteQueryModifierConfig().setFormat(format);
    }
}
