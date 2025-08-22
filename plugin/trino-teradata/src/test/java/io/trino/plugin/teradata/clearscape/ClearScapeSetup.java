package io.trino.plugin.teradata.clearscape;

import io.trino.plugin.teradata.util.TeradataTestConstants;

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
