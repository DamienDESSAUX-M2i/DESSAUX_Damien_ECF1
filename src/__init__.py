from .pipelines import BooksPipeline, PartenaireLibrairiesPipeline, QuotesPipeline
from .utils import set_up_logger

__all__ = [
    "set_up_logger",
    "QuotesPipeline",
    "PartenaireLibrairiesPipeline",
    "BooksPipeline",
]
