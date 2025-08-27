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

public class ClearScapeSetup
{
    private final String token;
    private final String password;
    private final String envName;
    private final boolean destoryEnv;
    private ClearScapeManager manager;

    public ClearScapeSetup(String token, String password, String envName, boolean destoryEnv)
    {
        this.token = token;
        this.password = password;
        this.envName = envName;
        this.destoryEnv = destoryEnv;
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
        return model;
    }

    public void cleanup()
    {
        if (destoryEnv && manager != null) {
            manager.teardown();
        }
    }
}
