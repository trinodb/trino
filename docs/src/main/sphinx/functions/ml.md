# Machine learning functions

The machine learning plugin provides machine learning functionality
as an aggregation function. It enables you to train Support Vector Machine (SVM)
based classifiers and regressors for the supervised learning problems.

:::{note}
The machine learning functions are not optimized for distributed processing.
The capability to train large data sets is limited by this execution of the
final training on a single instance.
:::

## Feature vector

To solve a problem with the machine learning technique, especially as a
supervised learning problem, it is necessary to represent the data set
with the sequence of pairs of labels and feature vector. A label is a
target value you want to predict from the unseen feature and a feature is a
A N-dimensional vector whose elements are numerical values. In Trino, a
feature vector is represented as a map-type value, whose key is an index
of each feature, so that it can express a sparse vector.
Since classifiers and regressors can recognize the map-type feature
vector, there is a function to construct the feature from the existing
numerical values, {func}`features`:

```
SELECT features(1.0, 2.0, 3.0) AS features;
```

```text
       features
-----------------------
 {0=1.0, 1=2.0, 2=3.0}
```

The output from {func}`features` can be directly passed to ML functions.

## Classification

Classification is a type of supervised learning problem to predict the distinct
label from the given feature vector. The interface looks similar to the
construction of the SVM model from the sequence of pairs of labels and features
implemented in Teradata Aster or [BigQuery ML](https://cloud.google.com/bigquery-ml/docs/bigqueryml-intro).
The function to train a classification model looks like as follows:

```
SELECT
  learn_classifier(
    species,
    features(sepal_length, sepal_width, petal_length, petal_width)
  ) AS model
FROM
  iris
```

It returns the trained model in a serialized format.

```text
                      model
-------------------------------------------------
 3c 43 6c 61 73 73 69 66 69 65 72 28 76 61 72 63
 68 61 72 29 3e
```

{func}`classify` returns the predicted label by using the trained model.
The trained model can not be saved natively, and needs to be passed in
the format of a nested query:

```
SELECT
  classify(features(5.9, 3, 5.1, 1.8), model) AS predicted_label
FROM (
  SELECT
    learn_classifier(species, features(sepal_length, sepal_width, petal_length, petal_width)) AS model
  FROM
    iris
) t
```

```text
 predicted_label
-----------------
 Iris-virginica
```

As a result you need to run the training process at the same time when predicting values.
Internally, the model is trained by [libsvm](https://www.csie.ntu.edu.tw/~cjlin/libsvm/).
You can use {func}`learn_libsvm_classifier` to control the internal parameters of the model.

## Regression

Regression is another type of supervised learning problem, predicting continuous
value, unlike the classification problem. The target must be numerical values that can
be described as `double`.

The following code shows the creation of the model predicting `sepal_length`
from the other 3 features:

```
SELECT
  learn_regressor(sepal_length, features(sepal_width, petal_length, petal_width)) AS model
FROM
  iris
```

The way to use the model is similar to the classification case:

```
SELECT
  regress(features(3, 5.1, 1.8), model) AS predicted_target
FROM (
  SELECT
    learn_regressor(sepal_length, features(sepal_width, petal_length, petal_width)) AS model
  FROM iris
) t;
```

```text
 predicted_target
-------------------
 6.407376822560477
```

Internally, the model is trained by [libsvm](https://www.csie.ntu.edu.tw/~cjlin/libsvm/).
{func}`learn_libsvm_regressor` provides you a way to control the training process.

## Machine learning functions

:::{function} features(double, ...) -> map(bigint, double)
Returns the map representing the feature vector.
:::

:::{function} learn_classifier(label, features) -> Classifier
Returns an SVM-based classifier model, trained with the given label and feature data sets.
:::

:::{function} learn_libsvm_classifier(label, features, params) -> Classifier
Returns an SVM-based classifier model, trained with the given label and feature data sets.
You can control the training process by libsvm parameters.
:::

:::{function} classify(features, model) -> label
Returns a label predicted by the given classifier SVM model.
:::

:::{function} learn_regressor(target, features) -> Regressor
Returns an SVM-based regressor model, trained with the given target and feature data sets.
:::

:::{function} learn_libsvm_regressor(target, features, params) -> Regressor
Returns an SVM-based regressor model, trained with the given target and feature data sets.
You can control the training process by libsvm parameters.
:::

:::{function} regress(features, model) -> target
Returns a predicted target value by the given regressor SVM model.
:::
