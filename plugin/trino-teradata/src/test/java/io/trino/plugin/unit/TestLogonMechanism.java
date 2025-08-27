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

package io.trino.plugin.unit;

import io.trino.plugin.teradata.LogonMechanism;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLogonMechanism
{
    @Test
    public void testFromStringValidValues()
    {
        assertThat(LogonMechanism.fromString("TD2")).isEqualTo(LogonMechanism.TD2);
        assertThat(LogonMechanism.fromString("JWT")).isEqualTo(LogonMechanism.JWT);
        assertThat(LogonMechanism.fromString("BEARER")).isEqualTo(LogonMechanism.BEARER);
        assertThat(LogonMechanism.fromString("SECRET")).isEqualTo(LogonMechanism.SECRET);

        // Case-insensitive
        assertThat(LogonMechanism.fromString("td2")).isEqualTo(LogonMechanism.TD2);
        assertThat(LogonMechanism.fromString("jwt")).isEqualTo(LogonMechanism.JWT);
    }

    @Test
    public void testGetMechanism()
    {
        assertThat(LogonMechanism.TD2.getMechanism()).isEqualTo("TD2");
        assertThat(LogonMechanism.JWT.getMechanism()).isEqualTo("JWT");
        assertThat(LogonMechanism.BEARER.getMechanism()).isEqualTo("BEARER");
        assertThat(LogonMechanism.SECRET.getMechanism()).isEqualTo("SECRET");
    }

    @Test
    public void testFromStringInvalidValue()
    {
        assertThatThrownBy(() -> LogonMechanism.fromString("UNKNOWN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown logon mechanism");
    }
}
