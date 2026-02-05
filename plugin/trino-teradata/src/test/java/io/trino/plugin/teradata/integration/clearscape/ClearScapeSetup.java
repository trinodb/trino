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
package io.trino.plugin.teradata.integration.clearscape;

import io.trino.plugin.teradata.integration.TeradataTestConstants;

import static java.util.Objects.requireNonNull;

public class ClearScapeSetup
{
    private final String token;
    private final String password;
    private final String envName;
    private final String region;
    private final boolean destroyEnv;
    private ClearScapeManager manager;

    public ClearScapeSetup(
            String token,
            String password,
            String envName,
            boolean destroyEnv,
            String region)
    {
        this.token = requireNonNull(token, "token is null");
        this.password = requireNonNull(password, "password is null");
        this.envName = requireNonNull(envName, "envName is null");
        this.region = requireNonNull(region, "region is null");
        this.destroyEnv = destroyEnv;
    }

    public Model initialize()
    {
        try {
            Model model = createModel();
            manager = new ClearScapeManager(model);
            manager.setup();
            return model;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize ClearScape environment: " + envName, e);
        }
    }

    private Model createModel()
    {
        return new Model(
                envName,
                null,
                TeradataTestConstants.CLEARSCAPE_USERNAME,
                password,
                TeradataTestConstants.CLEARSCAPE_USERNAME,
                token,
                region);
    }

    public void cleanup()
    {
        if (manager == null) {
            return;
        }
        if (destroyEnv) {
            manager.teardown();
            return;
        }
        manager.stop();
    }

    public EnvironmentResponse.State status()
    {
        if (manager == null) {
            throw new IllegalStateException("ClearScape manager is not initialized");
        }
        return manager.status();
    }
}
