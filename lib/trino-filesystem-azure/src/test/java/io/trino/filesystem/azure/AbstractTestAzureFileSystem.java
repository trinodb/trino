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
package io.trino.filesystem.azure;

import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.azure.AzureFileSystemTestingEnvironment.AccountKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestAzureFileSystem
        extends AbstractTestTrinoFileSystem
{
    protected static String getRequiredEnvironmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }

    private AzureFileSystemTestingEnvironment testingEnvironment;

    protected void initialize(String account, String accountKey, AccountKind expectedAccountKind)
            throws IOException
    {
        requireNonNull(account, "account is null");
        requireNonNull(accountKey, "accountKey is null");
        requireNonNull(expectedAccountKind, "expectedAccountKind is null");
        testingEnvironment = new AzureFileSystemTestingEnvironment(account, accountKey, expectedAccountKind);
    }

    @Override
    protected AbstractTrinoFileSystemTestingEnvironment testingEnvironment()
    {
        return testingEnvironment;
    }

    @AfterAll
    void tearDown()
    {
        if (testingEnvironment != null) {
            testingEnvironment.close();
        }
    }

    @AfterEach
    void afterEach()
    {
        testingEnvironment.cleanupFiles();
    }

    @Test
    @Override
    public void testPaths()
            throws IOException
    {
        // Azure file paths are always hierarchical
        testPathHierarchical();
    }
}
