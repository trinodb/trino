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

import io.trino.testing.containers.FlociGcp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGcsFileSystemFloci
        extends AbstractTestGcsFileSystem
{
    @Container
    private static final FlociGcp FLOCI_GCP = new FlociGcp();

    @BeforeAll
    void setup()
            throws IOException, GeneralSecurityException
    {
        TestingSigningCredentials credentials = new TestingSigningCredentials();
        GcsAuth gcsAuth = (builder, _) -> builder.setCredentials(credentials);
        initialize(
                new GcsFileSystemConfig()
                        .setEndpoint(Optional.of(FLOCI_GCP.getEndpoint().toString()))
                        .setProjectId(FLOCI_GCP_PROJECT_ID),
                gcsAuth);
    }
}
