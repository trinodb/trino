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
package io.trino.plugin.faker;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import jakarta.inject.Inject;
import net.datafaker.Faker;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.random.RandomGeneratorFactory;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.random.RandomGenerator.JumpableGenerator;

public class FakerPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Locale locale;
    private final JumpableGenerator jumpableRandom;
    private final Faker faker;

    @Inject
    public FakerPageSourceProvider(FakerConfig config)
    {
        locale = config.getLocale();
        // Every split should generate data in a sequence that does not overlap with other splits.
        // To make data generation deterministic, use a generator with the same seed,
        // but advance its state by a different offset for every split.
        // A jumpable random generator's state can be advanced forward by a big distance in a single call.
        // Xoroshiro128PlusPlus state has a period of 2^128, and a jump distance of 2^64.
        jumpableRandom = (JumpableGenerator) RandomGeneratorFactory.of("Xoroshiro128PlusPlus").create(1);
        faker = new Faker(locale, Random.from(jumpableRandom.copy()));
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        List<FakerColumnHandle> handles = columns
                .stream()
                .map(FakerColumnHandle.class::cast)
                .collect(toImmutableList());

        FakerSplit fakerSplit = (FakerSplit) split;
        Random random = random(fakerSplit.splitNumber());
        return new FakerPageSource(new Faker(locale, random), random, handles, fakerSplit.rowsOffset(), fakerSplit.rowsCount());
    }

    private Random random(long index)
    {
        JumpableGenerator jumpableRandom = this.jumpableRandom.copy();
        for (long i = 0; i < index; i++) {
            jumpableRandom.jump();
        }
        return Random.from(jumpableRandom);
    }

    public void validateGenerator(String generator)
    {
        faker.expression(generator);
    }
}
