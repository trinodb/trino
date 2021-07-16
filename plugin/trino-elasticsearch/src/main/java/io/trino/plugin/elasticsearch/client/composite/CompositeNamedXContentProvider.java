/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.trino.plugin.elasticsearch.client.composite;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.search.aggregations.Aggregation;

import java.util.List;

public class CompositeNamedXContentProvider
        implements NamedXContentProvider
{
    private static final List<NamedXContentRegistry.Entry> entryList;

    static {
        NamedXContentRegistry.Entry compositeParser = new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField("composite"),
                (p, c) -> ParsedComposite.fromXContent(p, (String) c));
        entryList = List.of(compositeParser);
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers()
    {
        return entryList;
    }
}
