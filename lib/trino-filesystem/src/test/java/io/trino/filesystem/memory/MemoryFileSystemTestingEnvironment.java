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
package io.trino.filesystem.memory;

import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoryFileSystemTestingEnvironment
        extends AbstractTrinoFileSystemTestingEnvironment
{
    private final MemoryFileSystem fileSystem;

    public MemoryFileSystemTestingEnvironment()
    {
        fileSystem = new MemoryFileSystem();
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    public TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("memory://");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        assertThat(fileSystem.isEmpty()).isTrue();
    }
}
