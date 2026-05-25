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
package io.trino.plugin.iceberg.encryption;

import io.trino.testing.containers.PrintingLogConsumer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;

final class AzureKeyVaultEmulator
        extends GenericContainer<AzureKeyVaultEmulator>
{
    // floci-az emulates only the Key Vault secrets API, but Iceberg key management requires the keys API (wrap/unwrap crypto operations)
    private static final String AZURE_KEY_VAULT_EMULATOR_IMAGE = "jamesgoulddev/azure-keyvault-emulator:3.1.0";

    private static final int AZURE_KEY_VAULT_EMULATOR_PORT = 4997;

    AzureKeyVaultEmulator()
    {
        super(DockerImageName.parse(AZURE_KEY_VAULT_EMULATOR_IMAGE));
        addExposedPort(AZURE_KEY_VAULT_EMULATOR_PORT);
        // The emulator requires a TLS certificate; copy the self-signed cert from test resources into the container.
        withCopyFileToContainer(MountableFile.forClasspathResource("azure-keyvault-emulator/emulator.pfx"), "/certs/emulator.pfx");
        waitingFor(Wait.forLogMessage(".*Application started.*\\n", 1));
        withLogConsumer(new PrintingLogConsumer("azure-keyvault-emulator"));
    }

    URI endpoint()
    {
        // The certificate's subject alternative names include localhost, so the host must resolve to localhost for TLS.
        return URI.create("https://localhost:%s".formatted(getMappedPort(AZURE_KEY_VAULT_EMULATOR_PORT)));
    }
}
