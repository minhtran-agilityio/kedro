import abc
from typing import Any, Callable

from kedro.io import AbstractTransformer

import logging


class ExampleTransformer(AbstractTransformer):
    @property
    def _logger(self):
        return logging.getLogger(self.__class__.__name__)

    def load(self, data_set_name: str, load: Callable[[], Any]) -> Any:
        """
        This method will be deprecated in Kedro 0.18.0 in favour of the Dataset Hooks
        `before_dataset_loaded` and `after_dataset_loaded`.

        Wrap the loading of a dataset.
        Call ``load`` to get the data from the data set / next transformer.

        Args:
            data_set_name: The name of the data set being loaded.
            load: A callback to retrieve the data being loaded from the
                data set / next transformer.

        Returns:
            The loaded data.
        """
        # pylint: disable=unused-argument, no-self-use

        self._logger.info("Begin the load Function")
        data = load()
        self._logger.info("End the load Function")
        return data

    def save(self, data_set_name: str, save: Callable[[Any], None], data: Any) -> None:
        """
        This method will be deprecated in Kedro 0.18.0 in favour of the Dataset Hooks
        `before_dataset_saved` and `after_dataset_saved`.

        Wrap the saving of a dataset.
        Call ``save`` to pass the data to the  data set / next transformer.

        Args:
            data_set_name: The name of the data set being saved.
            save: A callback to pass the data being saved on to the
                data set / next transformer.
            data: The data being saved
        """
        # pylint: disable=unused-argument, no-self-use
        self._logger.info("Begin the Save function")
        save(data)
        self._logger.info("End the Save function")
