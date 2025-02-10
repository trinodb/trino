# AI functions

## Overview

The AI functions allow you to invoke a large language model (LLM)
to perform various textual tasks.
Multiple LLM providers are supported: OpenAI and Ollama.

## Configuration

Because the AI functions require an external LLM service, they are not
available by default. To enable them, you must configure them using a
[catalog properties file](catalog-properties), which will register the functions
under the specified catalog name.

Create a catalog properties file `etc/catalog/ai.properties` that references
the `ai` connector:

```properties
connector.name=ai
```

The AI functions all use the `ai` schema name. To avoid needing to reference
the functions using their fully qualified name, you can configure the
`sql.path` [SQL environment property](/admin/properties-sql-environment)
in the `config.properties` file to include the `ai` catalog and schema:

```properties
sql.path=ai.ai
```

## Providers

### OpenAI

The OpenAI provider uses the [OpenAI API](https://platform.openai.com/docs/overview)
to perform the AI functions:

```properties
ai.provider=openai
ai.model=gpt-4o-mini
ai.openai.api-key=xxx
```

### Ollama

The Ollama provider uses [Ollama](https://ollama.com/) to perform the AI functions:

```properties
ai.provider=ollama
ai.model=llama3.3
ai.ollama.endpoint=http://localhost:11434
```

## Functions

:::{function} ai_analyze_sentiment(text) -> varchar
Analyzes the sentiment of the input text.

The sentiment will be one of
`'positive'`, `'negative'`, `'neutral'`, or `'mixed'`.

```sql
SELECT ai_analyze_sentiment('I love Trino');
-- positive
```
:::

:::{function} ai_classify(text, labels) -> varchar
Classifies the input text according to the provided labels.

```sql
SELECT ai_classify('Buy now!', ARRAY['spam', 'not spam']);
-- spam
```
:::

:::{function} ai_extract(text, labels) -> map(varchar, varchar)
Extracts values for the provided labels from the input text.

```sql
SELECT ai_extract('John is 25 years old', ARRAY['name', 'age']);
-- {name=John, age=25}
```
:::

:::{function} ai_fix_grammar(text) -> varchar
Corrects grammatical errors in the input text.

```sql
SELECT ai_fix_grammar('I are happy. What you doing?');
-- I am happy. What are you doing?
```
:::

:::{function} ai_gen(prompt) -> varchar
Generates text based on the input prompt.

```sql
SELECT ai_gen('Describe Trino in a few words');
-- Distributed SQL query engine.
```
:::

:::{function} ai_mask(text, labels) -> varchar
Masks values for the provided labels in the input text.

```sql
SELECT ai_mask(
    'Contact me at 555-1234 or visit us at 123 Main St.',
    ARRAY['phone', 'address']);
-- Contact me at [MASKED] or visit us at [MASKED].
```
:::

:::{function} ai_similarity(text1, text2) -> double
Calculates the similarity between two strings. The result is a value
between 0.0 and 1.0, where 1.0 indicates the strings are identical.

```sql
SELECT ai_similarity('The sky is blue', 'The car is red');
-- 0.23
```
:::

:::{function} ai_translate(text, language) -> varchar
Translates the input text to the specified language.

```sql
SELECT ai_translate('I like coffee', 'es');
-- Me gusta el café

SELECT ai_translate('I like coffee', 'zh-TW');
-- 我喜歡咖啡
```
:::
