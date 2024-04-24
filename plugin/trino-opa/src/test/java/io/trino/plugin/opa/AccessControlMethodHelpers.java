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
package io.trino.plugin.opa;

import io.trino.spi.security.AccessDeniedException;

import java.util.function.Consumer;
import java.util.function.Function;

public class AccessControlMethodHelpers
{
    private AccessControlMethodHelpers() {}

    public interface MethodWrapper
    {
        boolean isAccessAllowed(OpaAccessControl opaAccessControl);
    }

    public static class ThrowingMethodWrapper
            implements MethodWrapper
    {
        private final Consumer<OpaAccessControl> callable;

        public ThrowingMethodWrapper(Consumer<OpaAccessControl> callable)
        {
            this.callable = callable;
        }

        @Override
        public boolean isAccessAllowed(OpaAccessControl opaAccessControl)
        {
            try {
                this.callable.accept(opaAccessControl);
                return true;
            }
            catch (AccessDeniedException e) {
                if (!e.getMessage().contains("Access Denied")) {
                    throw new AssertionError("Expected AccessDenied exception to contain 'Access Denied' in the message");
                }
                return false;
            }
        }
    }

    public static class ReturningMethodWrapper
            implements MethodWrapper
    {
        private final Function<OpaAccessControl, Boolean> callable;

        public ReturningMethodWrapper(Function<OpaAccessControl, Boolean> callable)
        {
            this.callable = callable;
        }

        @Override
        public boolean isAccessAllowed(OpaAccessControl opaAccessControl)
        {
            return this.callable.apply(opaAccessControl);
        }
    }
}
