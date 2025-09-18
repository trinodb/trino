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

package io.trino.plugin.integration;

public class AuthenticationConfig
{
    private final String userName;
    private final String password;
    private final String jwtToken;
    private final String jwsPrivateKey;
    private final String jwsCertificate;
    private final String clientId;
    private final String clientSecret;

    public AuthenticationConfig()
    {
        this(null, null, null, null, null, null, null);
    }

    public AuthenticationConfig(String userName, String password)
    {
        this(userName, password, null, null, null, null, null);
    }

    public AuthenticationConfig(String userName, String password, String jwtToken, String jwsPrivateKey, String jwsCertificate,
            String clientId, String clientSecret)
    {
        this.userName = userName;
        this.password = password;
        this.jwtToken = jwtToken;
        this.jwsPrivateKey = jwsPrivateKey;
        this.jwsCertificate = jwsCertificate;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    // Getters only
    public String getUserName()
    {
        return userName;
    }

    public String getPassword()
    {
        return password;
    }

    public String getJwtToken()
    {
        return jwtToken;
    }

    public String getJwsPrivateKey()
    {
        return jwsPrivateKey;
    }

    public String getJwsCertificate()
    {
        return jwsCertificate;
    }

    public String getClientId()
    {
        return clientId;
    }

    public String getClientSecret()
    {
        return clientSecret;
    }
}
