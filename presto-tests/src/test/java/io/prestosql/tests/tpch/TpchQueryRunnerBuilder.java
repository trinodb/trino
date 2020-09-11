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
package io.prestosql.tests.tpch;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;

import java.util.Optional;
import java.util.function.Function;

import static io.prestosql.plugin.tpch.TpchConnectorFactory.TPCH_MAX_ROWS_PER_PAGE_PROPERTY;
import static io.prestosql.plugin.tpch.TpchConnectorFactory.TPCH_PRODUCE_PAGES;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class TpchQueryRunnerBuilder
        extends DistributedQueryRunner.Builder
{
    private static final Session DEFAULT_SESSION = testSessionBuilder()
            .setSource("test")
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private Optional<Integer> maxRowsPerPage = Optional.empty();
    private Optional<Boolean> producePages = Optional.empty();

    private TpchQueryRunnerBuilder()
    {
        super(DEFAULT_SESSION);
    }

    @Override
    public TpchQueryRunnerBuilder amendSession(Function<Session.SessionBuilder, Session.SessionBuilder> amendSession)
    {
        return (TpchQueryRunnerBuilder) super.amendSession(amendSession);
    }

    public TpchQueryRunnerBuilder withMaxRowsPerPage(int maxRowsPerPage)
    {
        this.maxRowsPerPage = Optional.of(maxRowsPerPage);
        return this;
    }

    public TpchQueryRunnerBuilder withProducePages(boolean producePages)
    {
        this.producePages = Optional.of(producePages);
        return this;
    }

    public static TpchQueryRunnerBuilder builder()
    {
        return new TpchQueryRunnerBuilder();
    }

    @Override
    public DistributedQueryRunner build()
            throws Exception
    {
        DistributedQueryRunner queryRunner = buildWithoutCatalogs();
        try {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            maxRowsPerPage.ifPresent(value -> properties.put(TPCH_MAX_ROWS_PER_PAGE_PROPERTY, value.toString()));
            producePages.ifPresent(value -> properties.put(TPCH_PRODUCE_PAGES, value.toString()));
            queryRunner.createCatalog("tpch", "tpch", properties.build());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public DistributedQueryRunner buildWithoutCatalogs()
            throws Exception
    {
        DistributedQueryRunner queryRunner = super.build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
