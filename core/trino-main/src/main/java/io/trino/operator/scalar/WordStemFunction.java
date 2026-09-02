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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlType;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.ArmenianStemmer;
import org.tartarus.snowball.ext.BasqueStemmer;
import org.tartarus.snowball.ext.CatalanStemmer;
import org.tartarus.snowball.ext.DanishStemmer;
import org.tartarus.snowball.ext.DutchStemmer;
import org.tartarus.snowball.ext.EnglishStemmer;
import org.tartarus.snowball.ext.FinnishStemmer;
import org.tartarus.snowball.ext.FrenchStemmer;
import org.tartarus.snowball.ext.GermanStemmer;
import org.tartarus.snowball.ext.HungarianStemmer;
import org.tartarus.snowball.ext.IrishStemmer;
import org.tartarus.snowball.ext.ItalianStemmer;
import org.tartarus.snowball.ext.LithuanianStemmer;
import org.tartarus.snowball.ext.NorwegianStemmer;
import org.tartarus.snowball.ext.PortugueseStemmer;
import org.tartarus.snowball.ext.RomanianStemmer;
import org.tartarus.snowball.ext.RussianStemmer;
import org.tartarus.snowball.ext.SpanishStemmer;
import org.tartarus.snowball.ext.SwedishStemmer;
import org.tartarus.snowball.ext.TurkishStemmer;

import java.util.Map;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class WordStemFunction
{
    private WordStemFunction() {}

    private static final Map<Slice, Supplier<SnowballStemmer>> STEMMERS = ImmutableMap.<Slice, Supplier<SnowballStemmer>>builder()
            .put(utf8Slice("ca"), CatalanStemmer::new)
            .put(utf8Slice("da"), DanishStemmer::new)
            .put(utf8Slice("de"), GermanStemmer::new)
            .put(utf8Slice("en"), EnglishStemmer::new)
            .put(utf8Slice("es"), SpanishStemmer::new)
            .put(utf8Slice("eu"), BasqueStemmer::new)
            .put(utf8Slice("fi"), FinnishStemmer::new)
            .put(utf8Slice("fr"), FrenchStemmer::new)
            .put(utf8Slice("hu"), HungarianStemmer::new)
            .put(utf8Slice("hy"), ArmenianStemmer::new)
            .put(utf8Slice("ir"), IrishStemmer::new)
            .put(utf8Slice("it"), ItalianStemmer::new)
            .put(utf8Slice("lt"), LithuanianStemmer::new)
            .put(utf8Slice("nl"), DutchStemmer::new)
            .put(utf8Slice("no"), NorwegianStemmer::new)
            .put(utf8Slice("pt"), PortugueseStemmer::new)
            .put(utf8Slice("ro"), RomanianStemmer::new)
            .put(utf8Slice("ru"), RussianStemmer::new)
            .put(utf8Slice("sv"), SwedishStemmer::new)
            .put(utf8Slice("tr"), TurkishStemmer::new)
            .buildOrThrow();

    @Description("Returns the stem of a word in the English language")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice wordStem(@SqlType("varchar(x)") Slice slice)
    {
        return wordStem(slice, new EnglishStemmer());
    }

    public static Slice wordStem(Slice slice, Slice language)
    {
        return wordStem(slice, createStemmer(language));
    }

    private static Slice wordStem(Slice slice, SnowballStemmer stemmer)
    {
        stemmer.setCurrent(slice.toStringUtf8());
        return stemmer.stem() ? utf8Slice(stemmer.getCurrent()) : slice;
    }

    private static SnowballStemmer createStemmer(Slice language)
    {
        Supplier<SnowballStemmer> stemmer = STEMMERS.get(language);
        if (stemmer == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unknown stemmer language: " + language.toStringUtf8());
        }
        return stemmer.get();
    }

    @Description("Returns the stem of a word in the given language")
    @ScalarFunction("word_stem")
    public static final class WithLanguage
    {
        private WithLanguage() {}

        @ScalarFunctionImplementationChoice
        public static final class Row
        {
            private Row() {}

            @LiteralParameters("x")
            @SqlType("varchar(x)")
            public static Slice wordStem(@SqlType("varchar(x)") Slice slice, @SqlType("varchar(2)") Slice language)
            {
                return WordStemFunction.wordStem(slice, language);
            }
        }

        @ConstantSpecialization(arguments = 1)
        public static final class ConstantLanguage
        {
            private final SnowballStemmer stemmer;

            public ConstantLanguage(@ConstantArgument(1) Slice language)
            {
                stemmer = createStemmer(language);
            }

            @LiteralParameters("x")
            @SqlType("varchar(x)")
            public Slice wordStem(@SqlType("varchar(x)") Slice slice)
            {
                return WordStemFunction.wordStem(slice, stemmer);
            }
        }
    }
}
