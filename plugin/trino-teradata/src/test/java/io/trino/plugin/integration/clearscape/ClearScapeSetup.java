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
package io.trino.plugin.integration.clearscape;

import io.trino.plugin.integration.util.TeradataTestConstants;

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
        requireNonNull(token, "token is null");
        requireNonNull(password, "password is null");
        requireNonNull(envName, "envName is null");
        requireNonNull(region, "region is null");
        this.token = token;
        this.password = password;
        this.envName = envName;
        this.region = region;
        this.destroyEnv = destroyEnv;
    }

    public Model initialize()
    {
        try {
            manager = new ClearScapeManager();
            Model model = createModel();
            manager.init(model);
            manager.setup();
            return model;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize ClearScape environment: " + envName, e);
        }
    }

    private Model createModel()
    {
        Model model = new Model();
        model.setEnvName(envName);
        model.setUserName(TeradataTestConstants.ENV_CLEARSCAPE_USERNAME);
        model.setPassword(password);
        model.setDatabaseName(TeradataTestConstants.ENV_CLEARSCAPE_USERNAME);
        model.setToken(token);
        model.setRegion(region);
        return model;
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
}
