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
package io.trino.plugin.varada.expression;

import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DomainExpression
        implements VaradaExpression
{
    private final VaradaVariable varadaVariable;
    private final Domain domain;

    public DomainExpression(VaradaVariable varadaVariable, Domain domain)
    {
        this.varadaVariable = varadaVariable;
        this.domain = domain;
    }

    @Override
    public List<? extends VaradaExpression> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public Type getType()
    {
        return domain.getType();
    }

    public Domain getDomain()
    {
        return domain;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DomainExpression that = (DomainExpression) o;
        return Objects.equals(varadaVariable, that.varadaVariable) && Objects.equals(domain, that.domain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(varadaVariable, domain);
    }

    @Override
    public String toString()
    {
        return "DomainExpression{" +
                "varadaVariable=" + varadaVariable +
                ", domain=" + domain +
                '}';
    }
}
