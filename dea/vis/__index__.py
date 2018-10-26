""" Data visualisation related utilities
"""
import numpy as np


def fill2d(xy, w):
    """ Rasterise 2d histogram.

    xy -- 2xN ndarray of integers representing (x,y) coordinates (negatives are ok)
    w  -  N element ndarray of weights

    returns:
    (image, extent)

    Where
       image: 2d ndarray
       extent: 4-tuple (left,right,bottom,top), compatible imshow(extent=)

    Use case: generate occupancy map of Albers tiles:
       - xy -- tile index
       - w -- number of datasets
    """
    assert xy.shape[0] == 2
    ranges = np.vstack([xy.min(axis=1),
                        xy.max(axis=1)]).T

    W, H = ranges[:2, 1] - ranges[:2, 0] + 1
    pad = np.r_[[-0.5, 0.5]]
    extent = tuple(ranges[0] + pad) + tuple(ranges[1] + pad)

    cc = np.zeros((H, W), dtype=w.dtype)
    x, y = xy - xy.min(axis=1).reshape(2, -1)
    cc[y, x] = w

    return cc, extent


def plot_poly(poly, ax, **kw):
    """ Add polygon to given matplotlib axis.
    """
    from descartes import PolygonPatch

    color = kw.get('ec', kw.get('fc', None))
    alpha = kw.get('alpha', None)
    marker = kw.pop('marker', '.')

    x, y = np.r_[poly.boundary.coords].T
    ax.add_patch(PolygonPatch(poly, **kw))
    ax.scatter(x, y, marker=marker, alpha=alpha, color=color)
