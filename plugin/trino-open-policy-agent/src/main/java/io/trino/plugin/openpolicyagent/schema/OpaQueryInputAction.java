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
package io.trino.plugin.openpolicyagent.schema;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OpaQueryInputAction(
        String operation,
        OpaQueryInputResource resource,
        List<OpaQueryInputResource> filterResources,
        OpaQueryInputResource targetResource,
        OpaQueryInputGrant grantee,
        TrinoGrantPrincipal grantor)
{
    private OpaQueryInputAction(OpaQueryInputAction.Builder builder)
    {
        this(builder.operation, builder.resource, builder.filterResources, builder.targetResource, builder.grantee, builder.grantor);
        if (this.resource != null && this.filterResources != null) {
            throw new IllegalArgumentException("resource and filterResources cannot both be configured");
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String operation;
        private OpaQueryInputResource resource;
        private List<OpaQueryInputResource> filterResources;
        private OpaQueryInputResource targetResource;
        private OpaQueryInputGrant grantee;
        private TrinoGrantPrincipal grantor;

        public Builder operation(String operation)
        {
            this.operation = operation;
            return this;
        }

        public Builder resource(OpaQueryInputResource resource)
        {
            this.resource = resource;
            return this;
        }

        public Builder filterResources(Collection<OpaQueryInputResource> resources)
        {
            this.filterResources = List.copyOf(resources);
            return this;
        }

        public Builder targetResource(OpaQueryInputResource targetResource)
        {
            this.targetResource = targetResource;
            return this;
        }

        public Builder grantee(OpaQueryInputGrant grantee)
        {
            this.grantee = grantee;
            return this;
        }

        public Builder grantor(TrinoGrantPrincipal grantor)
        {
            this.grantor = grantor;
            return this;
        }

        public OpaQueryInputAction build()
        {
            return new OpaQueryInputAction(this);
        }
    }
}
