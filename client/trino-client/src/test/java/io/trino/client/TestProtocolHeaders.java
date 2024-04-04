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

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.client.ProtocolHeaders.detectProtocol;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtocolHeaders
{
    @Test
    public void testDetection()
            throws Exception
    {
        // default is Trino
        assertThat(detectProtocol(Optional.empty(), ImmutableSet.of()).getProtocolName()).isEqualTo("Trino");

        // simple match
        assertThat(detectProtocol(Optional.of("Trino"), ImmutableSet.of("X-Trino-User")).getProtocolName()).isEqualTo("Trino");
        assertThat(detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Taco-User")).getProtocolName()).isEqualTo("Taco");
        assertThat(detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Taco-Source")).getProtocolName()).isEqualTo("Taco");

        // only specified header name is tested
        assertThat(detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Burrito-User", "X-Burrito-Source")).getProtocolName()).isEqualTo("Trino");

        // multiple protocols is not allowed
        assertThatThrownBy(() -> detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Taco-User", "X-Trino-Source")))
                .isInstanceOf(ProtocolDetectionException.class);
        assertThatThrownBy(() -> detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Trino-User", "X-Taco-User")))
                .isInstanceOf(ProtocolDetectionException.class);
    }
}
