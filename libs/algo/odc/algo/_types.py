from typing import Optional, Tuple, Union

import numpy as np

NumpyIndex1 = Union[int, slice]
NumpyIndex2 = Tuple[NumpyIndex1, NumpyIndex1]
NumpyIndex = Tuple[NumpyIndex1, ...]
NodataType = Union[int, float]
ShapeLike = Union[int, Tuple[int, ...]]
DtypeLike = Union[str, np.dtype]
