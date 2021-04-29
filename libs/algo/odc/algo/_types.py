import numpy as np
from typing import Union, Optional, Tuple

NumpyIndex1 = Union[int, slice]
NumpyIndex2 = Tuple[NumpyIndex1, NumpyIndex1]
NumpyIndex = Tuple[NumpyIndex1, ...]
NodataType = Union[int, float]
ShapeLike = Union[int, Tuple[int, ...]]
DtypeLike = Union[str, np.dtype]
