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
"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.17.0
"""

from typing import Dict
import pandas as pd

from .helpers import _is_true, _parse_money, _parse_percentage


def partition_by_day(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Split data frame into multiple files
    :param df:
    :return: Dict
    """
    parts = {}

    for day_of_month in df["DAY_OF_MONTH"].unique():
        parts[f"DAY_OF_MONTH=={day_of_month}"] = df[df["DAY_OF_MONTH"] == day_of_month]

    return parts


def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the data for companies.
    :param companies: Source data.
    :return: Preprocessed data.
    """
    companies["iata_approved"] = companies["iata_approved"].apply(_is_true)
    companies["company_rating"] = companies["company_rating"].apply(_parse_percentage)
    return companies
