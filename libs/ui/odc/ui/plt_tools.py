"""
Various data visualisation helper methods
"""
from matplotlib import pyplot as plt
from ._cmaps import scl_colormap

__all__ = (
    "scl_colormap",
    "compare_masks",
)


def compare_masks(
    a, b, names=("A", "B"), figsize=None, cmap="bone", interpolation="nearest", **kw
):
    """
    Plot two similar mask images on one figure, typically B is a transformed
    version of A.

    [   A  |    B   ]
    [added | removed]

    Where
    - ``added`` are pixels that turned True from A->B
    - ``removed`` are pixels that turned False from A->B
    """
    fig, axs = plt.subplots(2, 2, figsize=figsize)
    opts = dict(interpolation=interpolation, cmap=cmap, **kw)

    axs[0][0].set_title(names[0])
    axs[0][0].imshow(a, **opts)

    axs[0][1].set_title(names[1])
    axs[0][1].imshow(b, **opts)

    axs[1][0].set_title("Added")
    axs[1][0].imshow((~a) * b, **opts)

    axs[1][1].set_title("Removed")
    axs[1][1].imshow(a * (~b), **opts)

    axs[0][0].xaxis.set_visible(False)
    axs[0][1].xaxis.set_visible(False)
    axs[0][1].yaxis.set_visible(False)
    axs[1][1].yaxis.set_visible(False)

    fig.tight_layout(pad=0)
    return fig, axs
