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
package io.prestosql.server.ui;

import javax.ws.rs.container.ContainerRequestContext;

public interface WebUiAuthenticationManager
{
    static boolean isUiRequest(ContainerRequestContext request)
    {
        String path = request.getUriInfo().getRequestUri().getPath();
        return path == null || path.equals("/") || path.startsWith("/ui");
    }

    void handleUiRequest(ContainerRequestContext request);
}
