"""
Some color map data
"""
# flake8: noqa
import numpy as np

scl_colormap = np.array(
    [
        [255, 0, 255, 255],  # 0  - NODATA
        [255, 0, 4, 255],  # 1  - Saturated or Defective
        [0, 0, 0, 255],  # 2  - Dark Areas
        [97, 97, 97, 255],  # 3  - Cloud Shadow
        [3, 139, 80, 255],  # 4  - Vegetation
        [192, 132, 12, 255],  # 5  - Bare Ground
        [21, 103, 141, 255],  # 6  - Water
        [117, 0, 27, 255],  # 7  - Unclassified
        [208, 208, 208, 255],  # 8  - Cloud
        [244, 244, 244, 255],  # 9  - Definitely Cloud
        [195, 231, 240, 255],  # 10 - Thin Cloud
        [222, 157, 204, 255],  # 11 - Snow or Ice
    ],
    dtype="uint8",
)
