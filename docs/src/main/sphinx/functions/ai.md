# AI functions

The AI functions allow you to invoke a large language model (LLM) to perform
various textual tasks. Multiple LLM providers are supported, specifically
[OpenAI](https://platform.openai.com/) and
[Anthropic](https://www.anthropic.com/api) directly, and many others such as
Llama, DeepSeek, Phi, Mistral, or Gemma using [Ollama](https://ollama.com/).

The LLM must be provided outside Trino as an external service.

## Configuration

Because the AI functions require an external LLM service, they are not available
by default. To enable them, you must configure a [catalog properties
file](catalog-properties) to register the functions invoking the configured LLM
with the specified catalog name.

Create a catalog properties file `etc/catalog/llm.properties` that references
the `ai` connector:

```properties
connector.name=ai
```

The AI functions are available with the `ai` schema name. For the preceding
example, the functions use the `llm.ai` catalog and schema prefix.

To avoid needing to reference the functions with their fully qualified name,
configure the `sql.path` [SQL environment
property](/admin/properties-sql-environment) in the `config.properties` file to
include the catalog and schema prefix:

```properties
sql.path=llm.ai
```

Configure multiple catalogs to use the same functions with different LLM
providers. In this case, the functions must be referenced using their
fully qualified name, rather than relying on the SQL path.

### Providers

The AI functions invoke an external LLM. Access to the LLM API must be
configured in the catalog. Performance, results, and cost of all AI function
invocations are dependent on the LLM provider and the model used. You must
specify a model that is suitable for textual analysis.

:::{list-table} AI functions provider configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `ai.provider`
  - Required name of the provider. Must be `anthropic` for using the
    [Anthropic provider](ai-anthropic) or `openai` for [OpenAI](ai-openai) or
    [Ollama](ai-ollama).
* - `ai.anthropic.endpoint`
  - URL for the Anthropic API endpoint. Defaults to `https://api.anthropic.com`.
* - `ai.anthropic.api-key`
  - API key value for Anthropic API access. Required with `ai.provider` set to
    `anthropic`.
* - `ai.openai.endpoint`
  - URL for the OpenAI API or Ollama endpoint. Defaults to
    `https://api.openai.com`. Set to the URL endpoint for Ollama when using
    models via Ollama and add any string for the `ai.openai.api-key`.
* - `ai.openai.api-key`
  - API key value for OpenAI API access. Required with `ai.provider` set to
    `openai`. Required and ignored with Ollama use.
:::

The AI functions connect to the providers over HTTP. Configure the connection
using the `ai` prefix with the [](/admin/properties-http-client).

The following sections show minimal configurations for Anthropic, OpenAI, and
Ollama use.

(ai-anthropic)=
#### Anthropic 

The Anthropic provider uses the [Anthropic API](https://www.anthropic.com/api)
to perform the AI functions:

```properties
ai.provider=anthropic
ai.model=claude-3-5-sonnet-latest
ai.anthropic.api-key=xxx
```

Use [secrets](/security/secrets) to avoid actual API key values in the catalog
properties files.

(ai-openai)=
#### OpenAI

The OpenAI provider uses the [OpenAI API](https://platform.openai.com/)
to perform the AI functions:

```properties
ai.provider=openai
ai.model=gpt-4o-mini
ai.openai.api-key=xxx
```

Use [secrets](/security/secrets) to avoid actual API key values in the catalog
properties files.

(ai-ollama)=
#### Ollama

The OpenAI provider can be used with [Ollama](https://ollama.com/)
to perform the AI functions, as Ollama is compatible with the OpenAI API:

```properties
ai.provider=openai
ai.model=llama3.3
ai.openai.endpoint=http://localhost:11434
ai.openai.api-key=none
```

An API key must be specified, but is ignored by Ollama.

Ollama allows you to use [Llama, DeepSeek, Phi, Mistral, Gemma and other
models](https://ollama.com/search) on a self-hosted deployment or from a vendor.

### Model configuration

All providers support a number of different models. You must configure at least
one model to use for the AI function. The model must be suitable for textual
analysis. Provider and model choice impacts performance, results, and cost of
all AI functions.

Costs vary with AI function used based on the implementation prompt size, the
length of the input, and the length of the output from the model, because model
providers charge based input and output tokens.

Optionally configure different models from the same provider for each functions
as an override:

:::{list-table} AI function model configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `ai.model`
  - Required name of the model. Valid names vary by provider. Model must be
    suitable for textual analysis. The model is used for all functions, unless a
    specific model is configured for a function as override.
* - `ai.analyze-sentiment.model`
  - Optional override to use a different model for {func}`ai_analyze_sentiment`.
* - `ai.classify.model`
  - Optional override to use a different model for {func}`ai_classify`.
* - `ai.extract.model`
  - Optional override to use a different model for {func}`ai_extract`.
* - `ai.fix-grammar.model`
  - Optional override to use a different model for {func}`ai_fix_grammar`.
* - `ai.generate.model`
  - Optional override to use a different model for {func}`ai_gen`.
* - `ai.mask.model`
  - Optional override to use a different model for {func}`ai_mask`.
* - `ai.translate.model`
  - Optional override to use a different model for {func}`ai_translate`.
:::


## Functions

The following functions are available in each catalog configured with the `ai`
connector under the `ai` schema and use the configured LLM provider:

:::{function} ai_analyze_sentiment(text) -> varchar
Analyzes the sentiment of the input text.

The sentiment result is `positive`, `negative`, `neutral`, or `mixed`.

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
Masks the values for the provided labels in the input text by replacing them
with the text `[MASKED]`.

```sql
SELECT ai_mask(
    'Contact me at 555-1234 or visit us at 123 Main St.',
    ARRAY['phone', 'address']);
-- Contact me at [MASKED] or visit us at [MASKED].
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
