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
package io.trino.plugin.paimon;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonHandleJsonUtilsTest
{
    @Test
    public void testTypedHandleJsonFieldMustMatchExpectedHandleClass()
    {
        assertThatCode(() -> PaimonHandleJsonUtils.rejectUnknownHandleJsonField(
                "PaimonTableHandle",
                "@type",
                "paimon:io.trino.plugin.paimon.PaimonTableHandle"))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> PaimonHandleJsonUtils.rejectUnknownHandleJsonField(
                "PaimonTableHandle",
                "@type",
                "paimon:io.trino.plugin.paimon.PaimonColumnHandle"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid PaimonTableHandle JSON @type field");

        assertThatThrownBy(() -> PaimonHandleJsonUtils.rejectUnknownHandleJsonField(
                "PaimonTableHandle",
                "legacyField",
                true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown PaimonTableHandle JSON field: legacyField");
    }
}
