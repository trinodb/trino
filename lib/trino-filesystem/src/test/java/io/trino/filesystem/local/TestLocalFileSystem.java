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
package io.trino.filesystem.local;

import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocalFileSystem
        extends AbstractTestTrinoFileSystem
{
    private LocalFileSystemTestingEnvironment testingEnvironment;

    @BeforeAll
    void beforeAll()
            throws IOException
    {
        testingEnvironment = new LocalFileSystemTestingEnvironment();
    }

    @AfterEach
    void afterEach()
            throws IOException
    {
        testingEnvironment.cleanupFiles();
    }

    @AfterAll
    void afterAll()
            throws IOException
    {
        if (testingEnvironment != null) {
            testingEnvironment.close();
            testingEnvironment = null;
        }
    }

    @Override
    protected AbstractTrinoFileSystemTestingEnvironment testingEnvironment()
    {
        return testingEnvironment;
    }

    @Test
    void testPathsOutOfBounds()
    {
        assertThatThrownBy(() -> getFileSystem().newInputFile(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().newInputFile(createLocation("../file"), 22))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().newOutputFile(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().deleteFile(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().listFiles(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("../file"), createLocation("target")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("source"), createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
    }
}
