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
package io.trino.client;

import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;

import static io.trino.client.UserAgentBuilder.createUserAgent;
import static io.trino.client.UserAgentBuilder.sanitize;
import static org.assertj.core.api.Assertions.assertThat;

class TestUserAgentBuilder
{
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(' ').omitEmptyStrings().withKeyValueSeparator('=');

    @Test
    void testUserAgent()
    {
        String userAgent = createUserAgent("trino-cli", "1.0.0", Map.of("md/lang", "en-US", "md/feature", "experimental"));
        assertThat(userAgent).startsWith("trino-cli/1.0.0");

        Map<String, String> metadata = MAP_SPLITTER.split(userAgent.substring("trino-cli/1.0.0".length() + 1));
        assertThat(metadata)
                .containsEntry("lang/java", StandardSystemProperty.JAVA_VM_VERSION.value())
                .containsEntry("java/vm", sanitize(StandardSystemProperty.JAVA_VM_NAME.value()))
                .containsEntry("java/vendor", sanitize(StandardSystemProperty.JAVA_VENDOR.value()))
                .containsEntry("os", sanitize(StandardSystemProperty.OS_NAME.value()))
                .containsEntry("os/version", sanitize(StandardSystemProperty.OS_VERSION.value()))
                .containsEntry("arch", sanitize(StandardSystemProperty.OS_ARCH.value()))
                .containsEntry("locale", Locale.getDefault().toLanguageTag())
                .containsEntry("md/lang", "en-US")
                .containsEntry("md/feature", "experimental");
    }

    @Test
    void testSanitization()
    {
        assertThat(sanitize("")).isEmpty();
        assertThat(sanitize(" ")).isEqualTo("_");
        assertThat(sanitize("a")).isEqualTo("a");
        assertThat(sanitize("a b")).isEqualTo("a_b");
        assertThat(sanitize("a b c")).isEqualTo("a_b_c");
        assertThat(sanitize("a   b c")).isEqualTo("a_b_c");
        assertThat(sanitize("a -+   b c")).isEqualTo("a_-_b_c");
    }
}
