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
package io.trino.plugin.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.ws.rs.client.ClientBuilder;

public class PulsarAdminClientProvider
{
    private PulsarAdminClientProvider()
    { }

    public static PulsarAdmin getPulsarAdmin(PulsarConnectorConfig config) throws PulsarClientException
    {
        PulsarAdminBuilder builder = PulsarAdmin.builder();

        if (config.getAuthPlugin() != null && config.getAuthParams() != null) {
            builder.authentication(config.getAuthPlugin(), config.getAuthParams());
        }

        if (config.isTlsAllowInsecureConnection() != null) {
            builder.allowTlsInsecureConnection(config.isTlsAllowInsecureConnection());
        }

        if (config.isTlsHostnameVerificationEnable() != null) {
            builder.enableTlsHostnameVerification(config.isTlsHostnameVerificationEnable());
        }

        if (config.getTlsTrustCertsFilePath() != null) {
            builder.tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath());
        }

        builder.setContextClassLoader(ClientBuilder.class.getClassLoader());
        builder.serviceHttpUrl(config.getWebServiceUrl());

        return builder.build();
    }
}
