package io.trino.plugin.teradata.clearscape;

import io.airlift.log.Logger;

import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * Manager class responsible for provisioning, starting, and tearing down ClearScape environments
 * using the Teradata Environment API.
 * <p>
 * This class provides a complete lifecycle management for Teradata ClearScape environments,
 * including creation, starting, stopping, and deletion of environments. It reads configuration
 * from a JSON file, uses the environment configuration to call the Teradata ClearScape HTTP API,
 * and sets up the necessary JDBC parameters for connecting to a Teradata instance.
 * </p>
 * <p>
 * The manager ensures proper validation of environment URLs and handles various environment
 * states including running, stopped, and non-existent environments.
 * </p>
 *
 * @author Teradata Plugin Team
 * @version 1.0
 * @since 1.0
 */

public class ClearScapeManager
{
    private static final Logger log = Logger.get(ClearScapeManager.class);

    /**
     * Regular expression pattern to validate allowed ClearScape API URLs.
     * Only allows HTTPS/HTTP URLs pointing to api.clearscape.teradata.com domain.
     */
    private static final Pattern ALLOWED_URL_PATTERN =
            Pattern.compile("^(https?://)(www\\.)?api.clearscape.teradata\\.com.*");

    /** The model containing environment configuration and state */
    private Model model;

    /**
            * Default constructor for ClearScapeManager.
     * Initializes the manager without any configuration.
        */
    public ClearScapeManager()
    {
    }

    /**
     * Validates that the provided URL matches the expected Clearscape Teradata API pattern.
     * <p>
     * This method ensures that only legitimate Teradata ClearScape API endpoints are used,
     * preventing potential security issues from malicious URLs.
     * </p>
     *
     * @param url the environment URL to validate
     * @return {@code true} if the URL matches the allowed pattern, {@code false} otherwise
     * @throws NullPointerException if url is null
     */
    private boolean isValidUrl(String url)
    {
        return ALLOWED_URL_PATTERN.matcher(url).matches();
    }

    /**
     * Creates a new instance of {@link TeradataHttpClient} using the environment URL from the config.
     * <p>
     * This method validates the environment URL before creating the HTTP client to ensure
     * only valid Teradata ClearScape endpoints are used.
     * </p>
     *
     * @return an initialized {@link TeradataHttpClient} instance ready for API calls
     * @throws URISyntaxException if the environment URL is invalid or malformed
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

    /**
     * Initializes the ClearScape manager with the provided model configuration.
     * <p>
     * This method must be called before any environment operations can be performed.
     * The model contains necessary configuration such as environment name, token,
     * and other parameters required for ClearScape operations.
     * </p>
     *
     * @param model the configuration model containing environment details
     * @throws NullPointerException if model is null
     */
    public void init(Model model)
    {
        this.model = model;
    }

    /**
     * Sets up and starts the ClearScape environment.
     * <p>
     * This method is a convenience wrapper around {@link #createAndStartClearScapeInstance()}.
     * It handles the complete setup process including environment creation if needed
     * and starting the environment if it exists but is stopped.
     * </p>
     *
     * @throws RuntimeException if setup fails due to API errors or configuration issues
     * @throws IllegalStateException if the manager is not properly initialized
     */
    public void setup()
    {
        createAndStartClearScapeInstance();
    }

    /**
     * Stops the running ClearScape environment instance.
     * <p>
     * This method gracefully stops the environment without deleting it.
     * The environment can be restarted later using {@link #setup()}.
     * </p>
     *
     * @throws RuntimeException if stopping the instance fails due to API errors
     */
    public void stop()
    {
        stopClearScapeInstance();
    }

    /**
     * Shuts down and permanently deletes the ClearScape environment instance.
     * <p>
     * This method should be called to clean up resources after usage.
     * Once an environment is torn down, it cannot be recovered and must be
     * recreated if needed again.
     * </p>
     * <p>
     * <strong>Warning:</strong> This operation is irreversible and will permanently
     * delete all data and configurations in the environment.
     * </p>
     *
     * @throws RuntimeException if teardown fails due to API errors
     */
    public void teardown()
    {
        shutdownAndDestroyClearScapeInstance();
    }

    /**
     * Handles the logic for creating and starting a ClearScape environment instance.
     * <p>
     * This method implements the following logic:
     * <ul>
     *   <li>If the environment doesn't exist, creates a new one</li>
     *   <li>If the environment exists but is stopped, starts it</li>
     *   <li>If the environment is already running, no action is taken</li>
     * </ul>
     * </p>
     * <p>
     * Updates the model with host/IP and authentication information once the
     * environment is ready.
     * </p>
     *
     * @throws RuntimeException if creation or starting fails due to API errors,
     *                         network issues, or invalid configuration
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
     * Handles the logic for stopping a running ClearScape environment instance.
     * <p>
     * This method checks if the environment exists and is in a running state
     * before attempting to stop it. If the environment is already stopped
     * or doesn't exist, no action is taken.
     * </p>
     *
     * @throws RuntimeException if stopping the instance fails due to API errors
     *                         or network connectivity issues
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
     * Handles the logic for shutting down and permanently deleting a ClearScape environment.
     * <p>
     * This method attempts to delete the environment regardless of its current state.
     * If the environment doesn't exist, a warning is logged but no exception is thrown.
     * </p>
     * <p>
     * <strong>Note:</strong> This operation is permanent and cannot be undone.
     * </p>
     *
     * @throws RuntimeException if deletion fails due to API errors (excluding
     *                         environment not found errors which are logged as warnings)
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
