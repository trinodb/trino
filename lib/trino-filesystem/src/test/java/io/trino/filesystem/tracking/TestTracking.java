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
package io.trino.filesystem.tracking;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import org.junit.jupiter.api.Test;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestTracking
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(TrinoFileSystemFactory.class, TracingFileSystemFactory.class);
        assertAllMethodsOverridden(TrinoFileSystem.class, TrackingFileSystem.class);
        assertAllMethodsOverridden(TrinoInputFile.class, TrackingInputFile.class);
        assertAllMethodsOverridden(TrinoInput.class, TrackingInput.class);
        assertAllMethodsOverridden(TrinoOutputFile.class, TrackingOutputFile.class);
    }
}
