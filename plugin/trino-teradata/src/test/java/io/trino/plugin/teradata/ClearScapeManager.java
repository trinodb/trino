package io.trino.plugin.teradata;

import io.trino.plugin.teradata.clearscapeintegrations.BaseException;
import io.trino.plugin.teradata.clearscapeintegrations.ClearScapeEnvVariables;
import io.trino.plugin.teradata.clearscapeintegrations.CreateEnvironmentRequest;
import io.trino.plugin.teradata.clearscapeintegrations.DeleteEnvironmentRequest;
import io.trino.plugin.teradata.clearscapeintegrations.EnvironmentRequest;
import io.trino.plugin.teradata.clearscapeintegrations.EnvironmentResponse;
import io.trino.plugin.teradata.clearscapeintegrations.GetEnvironmentRequest;
import io.trino.plugin.teradata.clearscapeintegrations.OperationRequest;
import io.trino.plugin.teradata.clearscapeintegrations.TeradataHttpClient;
import io.trino.plugin.teradata.clearscapeintegrations.TeradataConstants;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.teradata.clearScapeIntegrations.BaseException;
import io.trino.plugin.teradata.clearScapeIntegrations.CreateEnvironmentRequest;
import io.trino.plugin.teradata.clearScapeIntegrations.DeleteEnvironmentRequest;
import io.trino.plugin.teradata.clearScapeIntegrations.EnvironmentRequest;
import io.trino.plugin.teradata.clearScapeIntegrations.EnvironmentResponse;
import io.trino.plugin.teradata.clearScapeIntegrations.GetEnvironmentRequest;
import io.trino.plugin.teradata.clearScapeIntegrations.OperationRequest;
import io.trino.plugin.teradata.clearScapeIntegrations.TeradataHttpClient;
import io.trino.plugin.teradata.clearScapeIntegrations.TeradataConstants;
import io.trino.plugin.teradata.clearScapeIntegrations.ClearScapeEnvVariables;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * Manager class responsible for provisioning, starting, and tearing down ClearScape environments
 * using the Teradata Environment API.
 * This class reads configuration from a JSON file, uses the environment configuration to call the
 * Teradata ClearScape HTTP API, and sets up the necessary JDBC parameters for connecting to a
 * Teradata instance.
 */
public class ClearScapeManager {

public class ClearScapeManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClearScapeManager.class);

    private static final Pattern ALLOWED_URL_PATTERN =
            Pattern.compile("^(https?://)(www\\.)?api.clearscape.teradata\\.com.*");


    /** Configuration object loaded from the JSON file. */
    private ObjectNode configJSON;

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
     * @param config the loaded JSON configuration
     * @return an initialized {@link TeradataHttpClient} instance
     * @throws URISyntaxException if the environment URL is invalid
     */
    private TeradataHttpClient getTeradataHttpClient(JsonNode config)
            throws URISyntaxException
    {
        String envUrl = ClearScapeEnvVariables.ENV_CLEARSCAPE_URL;
        if (isValidUrl(envUrl)) {
            return new TeradataHttpClient(envUrl);
        }
        else {
            throw new URISyntaxException(envUrl, "Provide valid environment URL");
        }
    }

    /**
     * Public method to create and start the ClearScape environment instance. Should be called
     * before any operations requiring an active environment.
     */
    public void setup()
    {
        this.configJSON = new ObjectMapper().createObjectNode();
        createAndStartClearScapeInstance();
    }

    /** Public method to stop the clearscape instance */
    public void stop() {
        stopClearScapeInstance();
    }

    /**
     * Public method to shut down and delete the ClearScape environment instance. Should be called
     * to clean up resources after usage.
     */
    public void teardown() {
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
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient(configJSON);
            String name = ClearScapeEnvVariables.ENV_CLEARSCAPE_NAME;
            String token = ClearScapeEnvVariables.ENV_CLEARSCAPE_TOKEN;

            EnvironmentResponse response = null;
            try {
                response = teradataHttpClient.getEnvironment(new GetEnvironmentRequest(name), token);
            }
            catch (BaseException be) {
                LOGGER.info("Environment {} is not available. {}", name, be.getMessage());
            }

            if (response == null || response.ip() == null) {
                CreateEnvironmentRequest request = new CreateEnvironmentRequest(
                        name,
                        ClearScapeEnvVariables.ENV_CLEARSCAPE_REGION,
                        ClearScapeEnvVariables.ENV_CLEARSCAPE_PASSWORD
                );
                response = teradataHttpClient.createEnvironment(request, token).get();
            }
            else if (response.state() == EnvironmentResponse.State.STOPPED) {
                EnvironmentRequest request = new EnvironmentRequest(name, new OperationRequest("start"));
                teradataHttpClient.startEnvironment(request, token);
            }

            if (response != null) {
                configJSON.put("host", response.ip());
            }

            ImmutableMap<String, Object> authMap = ImmutableMap.<String, Object>builder()
                    .put(TeradataConstants.AUTH_TYPE, "TD2")
                    .put("username", ClearScapeEnvVariables.ENV_CLEARSAOPE_USERNAME)
                    .put("password", ClearScapeEnvVariables.ENV_CLEARSCAPE_PASSWORD)
                    .build();

//            configJSON.set(TeradataConstants.LOG_MECH, Jsons.jsonNode(authMap));
            ObjectMapper mapper = new ObjectMapper();
            configJSON.set(TeradataConstants.LOG_MECH, mapper.valueToTree(authMap));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create and start ClearScape instance", e);
        }
    }

    /** Handles the logic for stopping a ClearScape environment instance. */
    private void stopClearScapeInstance() {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient(configJSON);
            String name = ClearScapeEnvVariables.ENV_CLEARSCAPE_NAME;
            String token = ClearScapeEnvVariables.ENV_CLEARSCAPE_TOKEN;

            EnvironmentResponse response = null;
            try {
                response = teradataHttpClient.getEnvironment(new GetEnvironmentRequest(name), token);
            } catch (BaseException be) {
                LOGGER.info("Environment {} is not available. {}", name, be.getMessage());
            }

            if (response != null &&
                    response.ip() != null &&
                    response.state() == EnvironmentResponse.State.RUNNING) {
                EnvironmentRequest request = new EnvironmentRequest(name, new OperationRequest("stop"));
                teradataHttpClient.stopEnvironment(request, token);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop ClearScape instance", e);
        }
    }

    /**
     * Handles the logic for shutting down and deleting a ClearScape environment instance. Logs a
     * warning if the environment is not available.
     */
    private void shutdownAndDestroyClearScapeInstance() {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient(configJSON);
            String token = ClearScapeEnvVariables.ENV_CLEARSCAPE_TOKEN;
            DeleteEnvironmentRequest request = new DeleteEnvironmentRequest(ClearScapeEnvVariables.ENV_CLEARSCAPE_NAME);
            teradataHttpClient.deleteEnvironment(request, token).get();
        } catch (BaseException be) {
            LOGGER.info("Environment {} is not available. Error - {}",
                   ClearScapeEnvVariables.ENV_CLEARSCAPE_NAME, be.getMessage());
        } catch (Exception e) {
            throw new RuntimeException("Failed to shutdown and destroy ClearScape instance", e);
        }
    }

    /**
     * Loads a JSON configuration file from the provided file path.
     *
     * @return the parsed {@link ObjectNode} representing the configuration
     * @throws RuntimeException if the file is missing or unreadable
     */
//    private ObjectNode loadConfig(String fileName) {
//        try {
//            if (Files.exists(Paths.get(fileName))) {
//                String content = Files.readString(Paths.get(fileName));
//                return (ObjectNode) Jsons.deserialize(content);
//            } else {
//                throw new IllegalArgumentException("Config file not found: " + fileName);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to load configuration from file: " + fileName, e);
//        }
//    }

    public ObjectNode getConfigJSON() {
        return configJSON;
    }
}

