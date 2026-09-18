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
package io.trino.filesystem.gcs;

import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static java.lang.reflect.Proxy.newProxyInstance;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGcsInput
{
    @Test
    void testZeroLengthReadTailDoesNotRead()
            throws Exception
    {
        Storage storage = (Storage) newProxyInstance(
                TestGcsInput.class.getClassLoader(),
                new Class<?>[] {Storage.class},
                (_, method, _) -> {
                    throw new AssertionError("Storage should not be accessed: " + method);
                });
        GcsInput input = new GcsInput(
                new GcsLocation(Location.of("gs://bucket/key")),
                storage,
                OptionalLong.empty(),
                Optional.empty());

        assertThat(input.readTail(new byte[0], 0, 0)).isEqualTo(0);
    }
}
