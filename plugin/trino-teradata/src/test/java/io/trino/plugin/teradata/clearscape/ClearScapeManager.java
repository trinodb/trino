package io.trino.plugin.teradata.clearscape;

import io.airlift.log.Logger;

import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * Manager class responsible for provisioning, starting, and tearing down ClearScape environments
 * using the Teradata Environment API.
 * This class reads configuration from a JSON file, uses the environment configuration to call the
 * Teradata ClearScape HTTP API, and sets up the necessary JDBC parameters for connecting to a
 * Teradata instance.
 */

public class ClearScapeManager
{
    private static final Logger log = Logger.get(ClearScapeManager.class);
    private static final Pattern ALLOWED_URL_PATTERN =
            Pattern.compile("^(https?://)(www\\.)?api.clearscape.teradata\\.com.*");
    private Model model;

    public ClearScapeManager()
    {
    }

    /**
     * Validates that the provided URL matches the expected Clearscape Teradata API pattern.
     *
     * @param url the environment URL to validate
     * @return true if the URL is valid, false otherwise
     */
    private boolean isValidUrl(String url)
    {
        return ALLOWED_URL_PATTERN.matcher(url).matches();
    }

    /**
     * Creates a new instance of {@link TeradataHttpClient} using the environment URL from the config.
     *
     * @return an initialized {@link TeradataHttpClient} instance
     * @throws URISyntaxException if the environment URL is invalid
     */
    private TeradataHttpClient getTeradataHttpClient()
            throws URISyntaxException
    {
        String envUrl = TeradataConstants.ENV_CLEARSCAPE_URL;
        if (isValidUrl(envUrl)) {
            return new TeradataHttpClient(envUrl);
        }
        else {
            throw new URISyntaxException(envUrl, "Provide valid environment URL");
        }
    }

    public void init(Model model)
    {
        this.model = model;
    }

    public void setup()
    {
        createAndStartClearScapeInstance();
    }

    /**
     * Public method to stop the clearscape instance
     */
    public void stop()
    {
        stopClearScapeInstance();
    }

    /**
     * Public method to shut down and delete the ClearScape environment instance. Should be called
     * to clean up resources after usage.
     */
    public void teardown()
    {
        shutdownAndDestroyClearScapeInstance();
    }

    /**
     * Handles the logic for creating and starting a ClearScape environment instance. If the
     * environment already exists and is stopped, it is started. If it doesn't exist, a new
     * environment is created. Updates the {@code configJSON} with host/IP and authentication info.
     */
    private void createAndStartClearScapeInstance()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();

            String token = this.model.getToken();
            String name = this.model.getEnvName();
            EnvironmentResponse response = null;
            try {
                response = teradataHttpClient.getEnvironment(new GetEnvironmentRequest(name), token);
            }
            catch (BaseException be) {
                log.info("Environment {} is not available. {}", name, be.getMessage());
            }

            if (response == null || response.ip() == null) {
                CreateEnvironmentRequest request = new CreateEnvironmentRequest(
                        name,
                        TeradataConstants.ENV_CLEARSCAPE_REGION,
                        model.getPassword());
                response = teradataHttpClient.createEnvironment(request, token).get();
            }
            else if (response.state() == EnvironmentResponse.State.STOPPED) {
                EnvironmentRequest request = new EnvironmentRequest(name, new OperationRequest("start"));
                teradataHttpClient.startEnvironment(request, token);
            }

            if (response != null) {
                model.setHostName(response.ip());
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create and start ClearScape instance", e);
        }
    }

    /**
     * Handles the logic for stopping a ClearScape environment instance.
     */
    private void stopClearScapeInstance()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();
            String token = this.model.getToken();
            String name = this.model.getEnvName();

            EnvironmentResponse response = null;
            try {
                response = teradataHttpClient.getEnvironment(new GetEnvironmentRequest(name), token);
            }
            catch (BaseException be) {
                log.info("Environment {} is not available. {}", name, be.getMessage());
            }
            if (response != null &&
                    response.ip() != null &&
                    response.state() == EnvironmentResponse.State.RUNNING) {
                EnvironmentRequest request = new EnvironmentRequest(name, new OperationRequest("stop"));
                teradataHttpClient.stopEnvironment(request, token);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to stop ClearScape instance", e);
        }
    }

    /**
     * Handles the logic for shutting down and deleting a ClearScape environment instance. Logs a
     * warning if the environment is not available.
     */
    private void shutdownAndDestroyClearScapeInstance()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();
            String token = this.model.getToken();
            DeleteEnvironmentRequest request = new DeleteEnvironmentRequest(TeradataConstants.ENV_CLEARSCAPE_NAME);
            teradataHttpClient.deleteEnvironment(request, token).get();
        }
        catch (BaseException be) {
            log.info("Environment {} is not available. Error - {}",
                    TeradataConstants.ENV_CLEARSCAPE_NAME, be.getMessage());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to shutdown and destroy ClearScape instance", e);
        }
    }
}
