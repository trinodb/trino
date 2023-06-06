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

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;

import static io.trino.server.ui.FormWebUiAuthenticationFilter.DISABLED_LOCATION_URI;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;

public class DisabledWebUiAuthenticationFilter
        implements WebUiAuthenticationFilter
{
    @Override
    public void filter(ContainerRequestContext request)
    {
        String path = request.getUriInfo().getRequestUri().getPath();
        if (path.equals("/")) {
            request.abortWith(Response.seeOther(DISABLED_LOCATION_URI).build());
            return;
        }

        // disabled page is always visible
        if (path.equals(FormWebUiAuthenticationFilter.DISABLED_LOCATION)) {
            return;
        }

        // send 401 to REST api calls and redirect to others
        if (path.startsWith("/ui/api/")) {
            request.abortWith(Response.status(UNAUTHORIZED).build());
        }
        else {
            // redirect to disabled page
            request.abortWith(Response.seeOther(DISABLED_LOCATION_URI).build());
        }
    }
}
