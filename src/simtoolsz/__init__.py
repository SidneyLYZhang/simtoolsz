import importlib.metadata

import simtoolsz.db as db
import simtoolsz.mail as mail
import simtoolsz.utils as utils
import simtoolsz.datetime as datetime
import simtoolsz.reader as reader


try:
    __version__ = importlib.metadata.version("simtoolsz")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.2.11.1"

__all__ = [
    '__version__', 'mail', 'utils', 'datetime', 'db', 'reader'

]