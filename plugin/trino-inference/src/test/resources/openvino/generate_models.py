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
Generates the ONNX models that the Trino inference connector tests serve with OpenVINO Model Server.
The models use standard ONNX operators only, so OpenVINO can load them, and their outputs are exact,
so the tests can assert them:

- iris: a multinomial logistic regression classifier for the iris dataset, with fixed coefficients.
  The input features has shape [-1, 4], the output label has shape [-1], and the output probabilities has shape [-1, 3].
- house-price, version 1: price = 1000 * area. Version 2: price = 1000 * area + 5000 * rooms.
  The inputs area (FP64) and rooms (INT64) and the output price (FP64) have shape [-1, 1].
- flags: inactive = NOT active, and doubled = 2 * count. The input active (BOOL) and count (INT32)
  and the outputs have shape [-1].
- matrix: returns its input. The input and the output have the shape [-1, 2, 2], which the connector does not support.

Run the script from this directory, with the onnx and numpy packages installed, to update the models.
"""

import json
import os

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

OPSET = [helper.make_opsetid("", 17)]
DIRECTORY = os.path.dirname(os.path.abspath(__file__))

# coefficients of a scikit-learn LogisticRegression trained on the iris dataset, rounded to two decimals, with 97% training accuracy.
# The features are sepal length, sepal width, petal length, and petal width, in cm, and the classes are setosa, versicolor, and virginica.
IRIS_WEIGHTS = np.array([
    [-0.42, 0.53, -0.11],
    [0.97, -0.32, -0.65],
    [-2.52, -0.21, 2.72],
    [-1.08, -0.94, 2.03],
], dtype=np.float32)
IRIS_BIAS = np.array([9.86, 2.23, -12.09], dtype=np.float32)


def save(name, version, graph):
    model = helper.make_model(graph, opset_imports=OPSET, producer_name="trino-inference-tests")
    model.ir_version = 8
    onnx.checker.check_model(model, full_check=True)
    path = os.path.join(DIRECTORY, "models", name, str(version), "model.onnx")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    onnx.save(model, path)


def iris():
    graph = helper.make_graph(
        nodes=[
            helper.make_node("MatMul", ["features", "weights"], ["products"]),
            helper.make_node("Add", ["products", "bias"], ["scores"]),
            helper.make_node("Softmax", ["scores"], ["probabilities"], axis=1),
            helper.make_node("ArgMax", ["scores"], ["label"], axis=1, keepdims=0),
        ],
        name="iris",
        inputs=[helper.make_tensor_value_info("features", TensorProto.FLOAT, ["rows", 4])],
        outputs=[
            helper.make_tensor_value_info("label", TensorProto.INT64, ["rows"]),
            helper.make_tensor_value_info("probabilities", TensorProto.FLOAT, ["rows", 3]),
        ],
        initializer=[numpy_helper.from_array(IRIS_WEIGHTS, "weights"), numpy_helper.from_array(IRIS_BIAS, "bias")])
    save("iris", 1, graph)


def house_price(version):
    nodes = [helper.make_node("Mul", ["area", "price_per_area"], ["area_price"])]
    initializers = [numpy_helper.from_array(np.array(1000, dtype=np.float64), "price_per_area")]
    if version == 1:
        nodes.append(helper.make_node("Identity", ["area_price"], ["price"]))
    else:
        nodes.append(helper.make_node("Cast", ["rooms"], ["rooms_double"], to=TensorProto.DOUBLE))
        nodes.append(helper.make_node("Mul", ["rooms_double", "price_per_room"], ["rooms_price"]))
        nodes.append(helper.make_node("Add", ["area_price", "rooms_price"], ["price"]))
        initializers.append(numpy_helper.from_array(np.array(5000, dtype=np.float64), "price_per_room"))
    graph = helper.make_graph(
        nodes=nodes,
        name="house_price",
        inputs=[
            helper.make_tensor_value_info("area", TensorProto.DOUBLE, ["rows", 1]),
            helper.make_tensor_value_info("rooms", TensorProto.INT64, ["rows", 1]),
        ],
        outputs=[helper.make_tensor_value_info("price", TensorProto.DOUBLE, ["rows", 1])],
        initializer=initializers)
    save("house-price", version, graph)


def flags():
    graph = helper.make_graph(
        nodes=[
            helper.make_node("Not", ["active"], ["inactive"]),
            helper.make_node("Add", ["count", "count"], ["doubled"]),
        ],
        name="flags",
        inputs=[
            helper.make_tensor_value_info("active", TensorProto.BOOL, ["rows"]),
            helper.make_tensor_value_info("count", TensorProto.INT32, ["rows"]),
        ],
        outputs=[
            helper.make_tensor_value_info("inactive", TensorProto.BOOL, ["rows"]),
            helper.make_tensor_value_info("doubled", TensorProto.INT32, ["rows"]),
        ])
    save("flags", 1, graph)


def matrix():
    graph = helper.make_graph(
        nodes=[helper.make_node("Identity", ["values"], ["same_values"])],
        name="matrix",
        inputs=[helper.make_tensor_value_info("values", TensorProto.FLOAT, ["rows", 2, 2])],
        outputs=[helper.make_tensor_value_info("same_values", TensorProto.FLOAT, ["rows", 2, 2])])
    save("matrix", 1, graph)


def config():
    models = [
        {"config": {"name": "iris", "base_path": "/models/iris"}},
        {"config": {"name": "house-price", "base_path": "/models/house-price", "model_version_policy": {"all": {}}}},
        {"config": {"name": "flags", "base_path": "/models/flags"}},
        {"config": {"name": "matrix", "base_path": "/models/matrix"}},
    ]
    with open(os.path.join(DIRECTORY, "models", "config.json"), "w") as file:
        json.dump({"model_config_list": models}, file, indent=2)
        file.write("\n")


if __name__ == "__main__":
    iris()
    house_price(1)
    house_price(2)
    flags()
    matrix()
    config()
