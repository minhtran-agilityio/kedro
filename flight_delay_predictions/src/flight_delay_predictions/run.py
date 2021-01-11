# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application entry point."""
from pathlib import Path
from typing import Dict

from kedro.framework.context import KedroContext, load_context
from kedro.framework.session import KedroSession
from kedro.extras.datasets.text import TextDataSet
from kedro.pipeline import Pipeline

from flight_delay_predictions.piplines.data_engineering import create_pipeline


class ProjectContext(KedroContext):
    """Users can override the remaining method from the parent class here,
    or create new ones

    Args:
        KedroContext ([type]): [description]
    """

    project_name = "flight_delay_predictions"
    project_version = "0.17.0"

    def _get_catalog(self, *args, **kwargs):
        catalog = super()._get_catalog(*args, **kwargs)

        my_datasets_file = self.config_loader.get("my_datasets*", "my_datasets*/**")
        my_datasets = my_datasets_file["my_datasets"]

        for dataset in my_datasets:
            catalog.add(dataset["name"], TextDataSet(filepath="data/data")),
            catalog.add(dataset["name"] + "_output", TextDataSet(filepath="data/data"))

        catalog.add("fake_data", TextDataSet(filepath="data/fake-data"))
        catalog.add("fake_data_output", TextDataSet(filepath="data/fake-data"))

        return catalog

    def _get_pipeline(self) -> Dict[str, Pipeline]:
        my_datasets_file = self.config_loader.get("my_datasets*")
        my_datasets = my_datasets_file["my_datasets"]

        return create_pipeline(my_datasets=my_datasets)


def run_package():
    # Entry point for running a Kedro project packaged with `kedro package`
    # using `python -m <project_package>.run` command.
    package_name = Path(__file__).resolve().parent.name
    with KedroSession.create(package_name) as session:
        session.run()


if __name__ == "__main__":
    run_package()
