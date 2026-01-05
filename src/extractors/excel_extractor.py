import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("app")


class ExcelExtractor:
    """Extractor for a Excel file."""

    def extract(self, file_path: Path, **kwargs) -> dict[str, pd.DataFrame]:
        """Extract data from a Exel file.

        Args:
            file_path (Path): Path of the Excel file.

        Returns:
            dict[str, pd.DataFrame]: Dictionary whose keys are the names of the sheets and whose values ​​are the loaded DataFrames.
        """
        try:
            logger.info(f"Attempting to extract data from {file_path}.")
            dict_dfs = pd.read_excel(io=file_path, sheet_name=None, **kwargs)
            logger.info(f"Extraction completed : {len(dict_dfs)} sheets extracted.")
            return dict_dfs
        except Exception as e:
            logger.error(f"Error CSV extractor: {e}.")
            raise
