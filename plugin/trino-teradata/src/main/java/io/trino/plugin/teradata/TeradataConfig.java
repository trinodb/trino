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

package io.trino.plugin.teradata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import java.util.Optional;

/**
 * Configuration class for Teradata connector properties.
 * <p>
 * This class holds configuration options related to Teradata JDBC behavior,
 * including transaction mode, character set, logon mechanism, case sensitivity,
 * and default database.
 * </p>
 * <p>
 * Many of these properties correspond to Teradata JDBC connection parameters.
 * </p>
 */
public class TeradataConfig
        extends BaseJdbcConfig
{
    private Optional<String> oidcJWTToken = Optional.empty();
    private Optional<String> oidcClientSecret = Optional.empty();
    private Optional<String> oidcJWSCertificate = Optional.empty();
    private Optional<String> oidcJWSPrivateKey = Optional.empty();
    private Optional<String> oidcClientId = Optional.empty();
    private String logMech = "TD2";
    private TeradataCaseSensitivity teradataCaseSensitivity = TeradataCaseSensitivity.CASE_SENSITIVE;

    public Optional<String> getOidcClientId()
    {
        return oidcClientId;
    }

    @Config("oidc.client-id")
    public TeradataConfig setOidcClientId(String clientId)
    {
        this.oidcClientId = Optional.ofNullable(clientId);
        return this;
    }

    public Optional<String> getOidcJWSPrivateKey()
    {
        return oidcJWSPrivateKey;
    }

    @Config("oidc.jws-private-key")
    public TeradataConfig setOidcJWSPrivateKey(String privateKey)
    {
        this.oidcJWSPrivateKey = Optional.ofNullable(privateKey);
        return this;
    }

    public Optional<String> getOidcJWSCertificate()
    {
        return oidcJWSCertificate;
    }

    @Config("oidc.jws-certificate")
    public TeradataConfig setOidcJWSCertificate(String certificate)
    {
        this.oidcJWSCertificate = Optional.ofNullable(certificate);
        return this;
    }

    public Optional<String> getOidcClientSecret()
    {
        return oidcClientSecret;
    }

    @Config("oidc.client-secret")
    public TeradataConfig setOidcClientSecret(String clientSecret)
    {
        this.oidcClientSecret = Optional.ofNullable(clientSecret);
        return this;
    }

    public Optional<String> getOidcJwtToken()
    {
        return oidcJWTToken;
    }

    @Config("jwt.token")
    public TeradataConfig setOidcJwtToken(String jwtToken)
    {
        this.oidcJWTToken = Optional.ofNullable(jwtToken);
        return this;
    }

    public String getLogMech()
    {
        return logMech;
    }

    @Config("logon-mechanism")
    @ConfigDescription("Specifies the logon mechanism for Teradata (default: TD2). Use 'TD2' for TD2 authentication.")
    public TeradataConfig setLogMech(String logMech)
    {
        this.logMech = logMech;
        return this;
    }

    /**
     * Gets the Teradata case sensitivity setting.
     *
     * @return the current TeradataCaseSensitivity mode (default: CASE_SPECIFIC)
     */
    public TeradataCaseSensitivity getTeradataCaseSensitivity()
    {
        return teradataCaseSensitivity;
    }

    /**
     * Sets how char/varchar columns' case sensitivity will be exposed to Trino.
     *
     * @param teradataCaseSensitivity the case sensitivity mode
     * @return this {@link TeradataConfig} instance for method chaining
     */
    @Config("teradata.case-sensitivity")
    @ConfigDescription("How char/varchar columns' case sensitivity will be exposed to Trino (default: CASE_SENSITIVE). Possible values: CASE_INSENSITIVE, CASE_SENSITIVE, AS_DEFINED.")
    public TeradataConfig setTeradataCaseSensitivity(TeradataCaseSensitivity teradataCaseSensitivity)
    {
        this.teradataCaseSensitivity = teradataCaseSensitivity;
        return this;
    }

    /**
     * Enum representing Teradata case sensitivity modes for char/varchar columns.
     * <ul>
     *   <li>CASE_INSENSITIVE - case insensitive</li>
     *   <li>CASE_SENSITIVE - case sensitive</li>
     *   <li>AS_DEFINED - as defined by Teradata</li>
     * </ul>
     */
    enum TeradataCaseSensitivity
    {
        CASE_INSENSITIVE, CASE_SENSITIVE, AS_DEFINED
    }
}
