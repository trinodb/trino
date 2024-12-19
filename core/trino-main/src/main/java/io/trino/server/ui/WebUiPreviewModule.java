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
package io.trino.server.ui;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.security.SecurityConfig;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.Locale.ENGLISH;

public class WebUiPreviewModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(WebUiPreviewStaticResource.class);
        String authentication = getAuthenticationType();
        switch (authentication) {
            case "insecure":
            case "form":
            case "jwt":
            case "kerberos":
            case "certificate":
                jaxrsBinder(binder).bind(LoginPreviewResource.class);
                break;
            case "fixed":
                jaxrsBinder(binder).bind(FixedUserPreviewResource.class);
                break;
            case "oauth2":
                jaxrsBinder(binder).bind(OAuth2WebUiPreviewResource.class);
                break;
            case "none":
                break;
            default:
                throw new IllegalArgumentException("Unknown authentication type: " + authentication);
        }
    }

    private String getAuthenticationType()
    {
        // Same as WebUiAuthenticationModule.getAuthenticationType()
        String authentication = buildConfigObject(WebUiAuthenticationConfig.class).getAuthentication();
        if (authentication != null) {
            return authentication.toLowerCase(ENGLISH);
        }

        // no authenticator explicitly set for the web ui, so choose a default:
        // If there is a password authenticator, use that.
        List<String> authenticationTypes = buildConfigObject(SecurityConfig.class).getAuthenticationTypes().stream()
                .map(type -> type.toLowerCase(ENGLISH))
                .collect(toImmutableList());
        if (authenticationTypes.contains("password")) {
            return "form";
        }
        // otherwise use the first authenticator type
        return authenticationTypes.stream().findFirst().orElseThrow(() -> new IllegalArgumentException("authenticatorTypes is empty"));
    }
}
