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
package io.trino.server;

import io.airlift.log.Logger;
import jakarta.annotation.Priority;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.ext.Provider;
import jakarta.ws.rs.ext.WriterInterceptor;
import jakarta.ws.rs.ext.WriterInterceptorContext;

import java.io.IOException;

@Provider
@Priority(11)
public class IoExceptionSuppressingWriterInterceptor
        implements WriterInterceptor
{
    private static final Logger log = Logger.get(IoExceptionSuppressingWriterInterceptor.class);

    @Override
    public void aroundWriteTo(WriterInterceptorContext context)
            throws WebApplicationException
    {
        try {
            context.proceed();
        }
        catch (IOException e) {
            if (e.getClass().getName().equalsIgnoreCase("org.eclipse.jetty.io.EofException")) {
                // EofException is thrown when the client closes the connection before the response is fully written.
                // This is not an error, so we suppress it.
                return;
            }
            log.warn("Could not write to output: %s(%s)", e.getClass().getSimpleName(), e.getMessage());
        }
    }
}
