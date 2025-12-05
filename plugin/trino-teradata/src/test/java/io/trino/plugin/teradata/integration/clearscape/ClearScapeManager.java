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

import io.airlift.log.Logger;
import io.trino.plugin.teradata.integration.TeradataTestConstants;

import java.net.URISyntaxException;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class ClearScapeManager
{
    private static final Logger log = Logger.get(ClearScapeManager.class);
    private static final Pattern ALLOWED_URL_PATTERN = Pattern.compile("^(https?://)(www\\.)?api.clearscape.teradata\\.com.*");
    private final Model model;

    public ClearScapeManager(Model model)
    {
        requireNonNull(model, "model should not be null");
        this.model = model;
    }

    public void setup()
    {
        createAndStartClearScapeInstance();
    }

    public void stop()
    {
        stopClearScapeInstance();
    }

    public EnvironmentResponse.State status()
    {
        return getClearScapeInstanceStatus();
    }

    public void teardown()
    {
        shutdownAndDestroyClearScapeInstance();
    }

    private EnvironmentResponse.State getClearScapeInstanceStatus()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();

            String token = model.getToken();
            String name = model.getEnvName();
            EnvironmentResponse response;
            try {
                response = teradataHttpClient.fetchEnvironment(new GetEnvironmentRequest(name), token);
            }
            catch (ClearScapeServiceException be) {
                return EnvironmentResponse.State.TERMINATED;
            }

            if (response != null) {
                return response.state();
            }
            return EnvironmentResponse.State.TERMINATED;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to get status of ClearScape instance", e);
        }
    }

    private void createAndStartClearScapeInstance()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();

            String token = model.getToken();
            String name = model.getEnvName();
            EnvironmentResponse response = null;
            try {
                response = teradataHttpClient.fetchEnvironment(new GetEnvironmentRequest(name), token);
            }
            catch (ClearScapeServiceException be) {
                log.info("Environment %s is not available. %s", name, be.getMessage());
            }

            if (response == null || response.ip() == null) {
                CreateEnvironmentRequest request = new CreateEnvironmentRequest(
                        name,
                        model.getRegion(),
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

    private void stopClearScapeInstance()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();
            String token = model.getToken();
            String name = model.getEnvName();

            EnvironmentResponse response = null;
            try {
                response = teradataHttpClient.fetchEnvironment(new GetEnvironmentRequest(name), token);
            }
            catch (ClearScapeServiceException be) {
                log.info("Environment %s is not available. %s", name, be.getMessage());
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

    private void shutdownAndDestroyClearScapeInstance()
    {
        try {
            TeradataHttpClient teradataHttpClient = getTeradataHttpClient();
            String token = model.getToken();
            DeleteEnvironmentRequest request = new DeleteEnvironmentRequest(model.getEnvName());
            teradataHttpClient.deleteEnvironment(request, token).get();
        }
        catch (ClearScapeServiceException be) {
            log.info("Environment %s is not available. Error - %s",
                    model.getEnvName(), be.getMessage());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to shutdown and destroy ClearScape instance", e);
        }
    }

    private TeradataHttpClient getTeradataHttpClient()
            throws URISyntaxException
    {
        String envUrl = TeradataTestConstants.CLEARSCAPE_URL;
        if (isValidUrl(envUrl)) {
            return new TeradataHttpClient(envUrl);
        }
        throw new URISyntaxException(envUrl, "Provide valid environment URL");
    }

    private static boolean isValidUrl(String url)
    {
        return ALLOWED_URL_PATTERN.matcher(url).matches();
    }
}
