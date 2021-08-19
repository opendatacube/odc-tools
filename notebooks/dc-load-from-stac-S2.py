# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: ODC
#     language: python
#     name: odc
# ---

# %%
import numpy as np
import planetary_computer as pc
import pystac
import yaml
from dask.distributed import wait as dask_wait
from datacube.model import Dataset
from datacube.storage import measurement_paths
from datacube.utils.dask import start_local_dask
from IPython.display import Image
from odc.algo import colorize, to_rgba
from odc.algo.io import dc_load
from odc.stac import stac2ds
from odc.ui import to_jpeg_data
from odc.ui.plt_tools import scl_colormap
from pystac_client import Client

cfg = """---
sentinel-2-l2a:
  measurements:
    '*':
      dtype: uint16
      nodata: 0
      units: '1'
    SCL:
      dtype: uint8
      nodata: 0
      units: '1'
    visual:
      dtype: uint8
      nodata: 0
      units: '1'
"""
cfg = yaml.load(cfg, Loader=yaml.CSafeLoader)

# pc.sign(Dataset)
#  operates in-place, maybe fix that?
@pc.sign.register(Dataset)
def sign_odc_ds(ds: Dataset) -> Dataset:
    for m in ds.metadata_doc["measurements"].values():
        m["path"] = pc.sign(m["path"])
    return ds


# %% [markdown]
# ## Start Dask Client

# %%
client = start_local_dask(mem_safety_margin="4G")
client

# %%
catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

RGB = ("B04", "B03", "B02")

query = catalog.search(
    collections=["sentinel-2-l2a"],
    datetime="2019-06",
    query={"s2:mgrs_tile": dict(eq="06VVN")},
)

dss = map(pc.sign, stac2ds(query.get_items(), cfg))
dss = list(dss)

print(f"Found: {len(dss):d} datasets")

# %%
# automatic CRS/resolution selection will come later
SHRINK = 4
crs = dss[0].crs
resolution = (-10, 10)

if SHRINK > 1:
    resolution = tuple(r * SHRINK for r in resolution)

xx = dc_load(dss, chunks={"x": 2048, "y": 2048}, output_crs=crs, resolution=resolution)
display(xx)

scl_rgb = colorize(xx.SCL, scl_colormap)
im_rgba = to_rgba(xx, clamp=(1, 3_000), bands=RGB)

display(scl_rgb)
display(im_rgba)

# %% [markdown]
# ## Load all the data into Dask Cluster

# %%
# %%time

scl_rgb, im_rgba = client.persist([scl_rgb, im_rgba])
_ = dask_wait([scl_rgb, im_rgba])

# %% [markdown]
# ## Plot Imagery

# %%
scl_rgb.isel(time=np.s_[:9]).compute().plot.imshow(
    col="time", col_wrap=3, size=4, aspect=1, interpolation="antialiased"
);

# %%
fig = (
    im_rgba.isel(time=np.s_[:9])
    .compute()
    .plot.imshow(col="time", col_wrap=3, size=4, aspect=1, interpolation="bicubic")
)
for ax in fig.axes.ravel():
    ax.set_facecolor("magenta")

# %%
idx=4
display(Image(data=to_jpeg_data(im_rgba.isel(time=idx).data.compute(), 90)))
display(Image(data=to_jpeg_data(scl_rgb.isel(time=idx).data.compute(), 90)))

# %% [markdown]
# -----------------------------
