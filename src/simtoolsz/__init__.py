import importlib.metadata

import simtoolsz.io as io
import simtoolsz.mail as mail
import simtoolsz.utils as utils
import simtoolsz.datetime as datetime


try:
    __version__ = importlib.metadata.version("simtoolsz")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.2.2"

__all__ = [
    '__version__', 'mail', 'utils', 'datetime', 'io'

]