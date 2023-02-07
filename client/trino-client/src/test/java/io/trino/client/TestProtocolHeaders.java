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
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.client.ProtocolHeaders.detectProtocol;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestProtocolHeaders
{
    @Test
    public void testDetection()
            throws Exception
    {
        // default is Trino
        assertEquals(detectProtocol(Optional.empty(), ImmutableSet.of()).getProtocolName(), "Trino");

        // simple match
        assertEquals(detectProtocol(Optional.of("Trino"), ImmutableSet.of("X-Trino-User")).getProtocolName(), "Trino");
        assertEquals(detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Taco-User")).getProtocolName(), "Taco");
        assertEquals(detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Taco-Source")).getProtocolName(), "Taco");

        // only specified header name is tested
        assertEquals(detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Burrito-User", "X-Burrito-Source")).getProtocolName(), "Trino");

        // multiple protocols is not allowed
        assertThatThrownBy(() -> detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Taco-User", "X-Trino-Source")))
                .isInstanceOf(ProtocolDetectionException.class);
        assertThatThrownBy(() -> detectProtocol(Optional.of("Taco"), ImmutableSet.of("X-Trino-User", "X-Taco-User")))
                .isInstanceOf(ProtocolDetectionException.class);
    }
}
