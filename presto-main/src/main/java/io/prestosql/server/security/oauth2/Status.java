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
package io.prestosql.server.security.oauth2;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class Status<T extends Challenge>
{
    static final Status<Challenge.Started> STARTED = new Status(Challenge.Started.class, false);
    static final Status<Challenge.Succeeded> SUCCEEDED = new Status(Challenge.Succeeded.class, true);
    static final Status<Challenge.Failed> FAILED = new Status(Challenge.Failed.class, true);

    private final Class<T> correspondingChallengeClass;
    private final boolean isFinal;

    private Status(Class<T> correspondingChallengeClass, boolean isFinal)
    {
        this.correspondingChallengeClass = requireNonNull(correspondingChallengeClass, "correspondingChallengeClass is null");
        this.isFinal = isFinal;
    }

    public Optional<T> toStatus(Challenge challenge)
    {
        return Optional.of(challenge)
                .filter(correspondingChallengeClass::isInstance)
                .map(correspondingChallengeClass::cast);
    }

    boolean isFinal()
    {
        return isFinal;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Status)) {
            return false;
        }
        Status<?> status = (Status<?>) o;
        return Objects.equals(correspondingChallengeClass, status.correspondingChallengeClass);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(correspondingChallengeClass);
    }
}
