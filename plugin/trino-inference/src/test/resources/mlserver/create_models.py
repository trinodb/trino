# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Creates the scikit-learn models that the Trino inference connector tests serve with MLServer, in the directory given as the argument.
MLServer loads scikit-learn models with pickle, which requires the scikit-learn version that saved them, so the tests run the script
in the MLServer container, before they start MLServer:

- house-price: a linear regression for price = 1000 * area + 5000 * rooms, trained on a pandas DataFrame with the columns area and rooms.
  The model settings declare the inputs area (FP64) and rooms (INT64), and select the pandas codec, which decodes the inputs as the
  columns of a DataFrame. They declare the output predict (FP64). All tensors have shape [-1, 1].
- iris: a logistic regression classifier for the iris dataset. The input features has shape [-1, 4], the output predict has
  shape [-1, 1], and the output predict_proba has shape [-1, 3]. The scikit-learn runtime returns only predict, unless the
  request names the outputs.
- iris-probabilities: the iris model, with model settings that declare only the output predict_proba.
- iris-variable-length: the iris model, with the input shape [-1, -1], which allows any number of values per row.
- undeclared-metadata: the house-price model, with model settings that do not declare the inputs and outputs,
  so its metadata does not have any.

The following models have metadata that does not match the model, or that the connector does not support:

- fixed-rows: the iris model, with the input shape [1, 4], which has a fixed number of rows.
- unsupported-data-type: the iris model, with the output data type UINT64.
- wrong-output-shape: the iris model, with the shape [-1, 2] of the output predict_proba, which has 3 values per row.
- wrong-output-data-type: the house-price model, with the data type INT64 of the output predict, which has FP64 values.
"""

import json
import os
import sys

import joblib
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.linear_model import LinearRegression, LogisticRegression


def save(directory, name, model, settings):
    path = os.path.join(directory, name)
    os.makedirs(path, exist_ok=True)
    joblib.dump(model, os.path.join(path, "model.joblib"))
    parameters = {"uri": "./model.joblib", **settings.get("parameters", {})}
    settings = {"name": name, "implementation": "mlserver_sklearn.SKLearnModel", **settings, "parameters": parameters}
    with open(os.path.join(path, "model-settings.json"), "w") as file:
        json.dump(settings, file, indent=2)


def house_price():
    houses = pd.DataFrame({"area": [50.0, 100.0, 150.0, 200.0], "rooms": [1, 3, 2, 4]})
    return LinearRegression().fit(houses, 1000 * houses["area"] + 5000 * houses["rooms"])


def iris():
    features, species = load_iris(return_X_y=True)
    return LogisticRegression(max_iter=1000).fit(features, species)


if __name__ == "__main__":
    directory = sys.argv[1]
    house_price_model = house_price()
    house_price_inputs = [
        {"name": "area", "datatype": "FP64", "shape": [-1, 1]},
        {"name": "rooms", "datatype": "INT64", "shape": [-1, 1]},
    ]
    save(directory, "house-price", house_price_model, {
        "inputs": house_price_inputs,
        "outputs": [{"name": "predict", "datatype": "FP64", "shape": [-1, 1]}],
        "parameters": {"content_type": "pd"},
    })
    iris_model = iris()
    iris_input = {"name": "features", "datatype": "FP64", "shape": [-1, 4]}
    label = {"name": "predict", "datatype": "INT64", "shape": [-1, 1]}
    probabilities = {"name": "predict_proba", "datatype": "FP64", "shape": [-1, 3]}
    save(directory, "iris", iris_model, {"inputs": [iris_input], "outputs": [label, probabilities]})
    save(directory, "iris-probabilities", iris_model, {"inputs": [iris_input], "outputs": [probabilities]})
    save(directory, "iris-variable-length", iris_model, {"inputs": [{**iris_input, "shape": [-1, -1]}], "outputs": [label]})
    save(directory, "undeclared-metadata", house_price_model, {})

    save(directory, "fixed-rows", iris_model, {"inputs": [{**iris_input, "shape": [1, 4]}], "outputs": [label]})
    save(directory, "unsupported-data-type", iris_model, {"inputs": [iris_input], "outputs": [{**label, "datatype": "UINT64"}]})
    save(directory, "wrong-output-shape", iris_model, {"inputs": [iris_input], "outputs": [{**probabilities, "shape": [-1, 2]}]})
    save(directory, "wrong-output-data-type", house_price_model, {
        "inputs": house_price_inputs,
        "outputs": [{"name": "predict", "datatype": "INT64", "shape": [-1, 1]}],
        "parameters": {"content_type": "pd"},
    })
