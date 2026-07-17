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
package io.trino.execution.admission;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.AdmissionPolicyFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Coverage for factory failure modes: a factory that throws and a factory that
 * returns null, plus duplicate-name detection.
 *
 * <p>Feature: admission-policy-spi, Property 5: Plugin discovery and binding correctness
 * — failure-mode branches.
 *
 * <p>Validates: Requirements 3.3, 3.6
 */
public class TestAdmissionPolicyFactoryFailures
{
    private static final String FAILING_FACTORY_NAME = "failing-factory";

    @Test
    public void throwingFactoryFailsStartupWithCausePreserved()
    {
        IllegalStateException root = new IllegalStateException("boom from the stub factory");
        ThrowingFactory factory = new ThrowingFactory(root);

        Throwable assertionTarget = catchInjectorError(FAILING_FACTORY_NAME, ImmutableList.of(factory));

        assertThat(assertionTarget).isNotNull();
        assertThat(messageChain(assertionTarget)).contains(FAILING_FACTORY_NAME);
        assertThat(messageChain(assertionTarget)).contains(ThrowingFactory.class.getName());
        assertThat(rootCauseChain(assertionTarget)).contains(root);
    }

    @Test
    public void factoryReturningNullFailsStartupWithFactoryNameInMessage()
    {
        NullReturningFactory factory = new NullReturningFactory();

        Throwable assertionTarget = catchInjectorError(FAILING_FACTORY_NAME, ImmutableList.of(factory));

        assertThat(assertionTarget).isNotNull();
        assertThat(messageChain(assertionTarget))
                .contains(FAILING_FACTORY_NAME)
                .contains(NullReturningFactory.class.getName())
                .contains("returned null");
    }

    @Test
    public void duplicateFactoryNameFailsStartup()
    {
        DuplicateFactoryA first = new DuplicateFactoryA();
        DuplicateFactoryB second = new DuplicateFactoryB();
        assertThat(first.getName()).isEqualTo(second.getName());

        Throwable assertionTarget = catchInjectorError(first.getName(), ImmutableList.of(first, second));

        assertThat(assertionTarget).isNotNull();
        String chain = messageChain(assertionTarget);
        assertThat(chain).contains(first.getName());
        assertThat(chain).contains(DuplicateFactoryA.class.getName());
        assertThat(chain).contains(DuplicateFactoryB.class.getName());
    }

    private static Throwable catchInjectorError(String configuredName, ImmutableList<AdmissionPolicyFactory> factories)
    {
        try {
            Injector injector = TestAdmissionPolicyFactoryLoading.boot(configuredName, factories);
            // If initialization completed without failure, force an instantiation
            // to surface the provider error (defensive — initialize() should
            // already have failed).
            injector.getInstance(AdmissionPolicy.class);
            return null;
        }
        catch (Throwable t) {
            return t;
        }
    }

    /**
     * Walks {@code throwable.getCause()} and concatenates each level's message
     * so callers can assert against the full failure chain in one go. Guice
     * wraps user-thrown failures inside a {@code CreationException}, and the
     * Trino factory wrapper adds another layer; both levels carry information
     * the tests need to validate.
     */
    private static String messageChain(Throwable throwable)
    {
        StringBuilder builder = new StringBuilder();
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (builder.length() > 0) {
                builder.append('\n');
            }
            builder.append(t.getClass().getName());
            String message = t.getMessage();
            if (message != null) {
                builder.append(": ").append(message);
            }
        }
        return builder.toString();
    }

    private static List<Throwable> rootCauseChain(Throwable throwable)
    {
        List<Throwable> result = new ArrayList<>();
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            result.add(t);
        }
        return result;
    }

    private static final class ThrowingFactory
            implements AdmissionPolicyFactory
    {
        private final RuntimeException toThrow;

        private ThrowingFactory(RuntimeException toThrow)
        {
            this.toThrow = toThrow;
        }

        @Override
        public String getName()
        {
            return FAILING_FACTORY_NAME;
        }

        @Override
        public AdmissionPolicy create(Map<String, String> config)
        {
            throw toThrow;
        }
    }

    private static final class NullReturningFactory
            implements AdmissionPolicyFactory
    {
        @Override
        public String getName()
        {
            return FAILING_FACTORY_NAME;
        }

        @Override
        public AdmissionPolicy create(Map<String, String> config)
        {
            return null;
        }
    }

    private static final class DuplicateFactoryA
            implements AdmissionPolicyFactory
    {
        @Override
        public String getName()
        {
            return "duplicate-name";
        }

        @Override
        public AdmissionPolicy create(Map<String, String> config)
        {
            throw new UnsupportedOperationException("uniqueness check should fail before this is invoked");
        }
    }

    private static final class DuplicateFactoryB
            implements AdmissionPolicyFactory
    {
        @Override
        public String getName()
        {
            return "duplicate-name";
        }

        @Override
        public AdmissionPolicy create(Map<String, String> config)
        {
            throw new UnsupportedOperationException("uniqueness check should fail before this is invoked");
        }
    }
}
