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
package io.trino.metadata;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record RedirectionAwareViewHandle(
        // the target view name after redirection. Optional.empty() if the view is not redirected.
        Optional<QualifiedObjectName> redirectedViewName)
{
    public static RedirectionAwareViewHandle withRedirectionTo(QualifiedObjectName redirectedViewName)
    {
        return new RedirectionAwareViewHandle(Optional.of(redirectedViewName));
    }

    public static RedirectionAwareViewHandle noRedirection()
    {
        return new RedirectionAwareViewHandle(Optional.empty());
    }

    public RedirectionAwareViewHandle
    {
        requireNonNull(redirectedViewName, "redirectedViewName is null");
    }
}
