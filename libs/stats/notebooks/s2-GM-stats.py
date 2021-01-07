# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %matplotlib inline
from IPython.display import display, Image

import matplotlib.pyplot as plt

plt.rcParams["axes.facecolor"] = "magenta"  # makes transparent pixels obvious
import numpy as np
import xarray as xr

# %%
from dask.distributed import Client, wait as dask_wait
from datacube.utils.dask import start_local_dask
from datacube.utils.rio import configure_s3_access

if False:
    client = start_local_dask(scheduler_port=11311, threads_per_worker=16)
    configure_s3_access(aws_unsigned=True, cloud_defaults=True, client=client)
else:
    client = Client("tcp://127.0.0.1:11311")

client.restart()
client

# %%
from odc.algo import to_rgba
from odc.ui import to_jpeg_data


def mk_roi(y, x, sz=256):
    return np.s_[y : y + sz, x : x + sz]


def show_im(data, transparent=(255, 0, 255)):
    display(Image(data=to_jpeg_data(data, transparent=transparent)))


# %%
from odc.stats.tasks import TaskReader
from odc.stats._gm import gm_product, gm_input_data, gm_reduce

RGB = ("B04", "B03", "B02")

product = gm_product()

# %%
tidx = ("2019--P1Y", 4, 13)
rdr = TaskReader("s2_2019.db", product=product)
rdr

# %%
task = rdr.load_task(tidx)
task

# %%
# %%time
# This builds a large Dask graph, can take some time

xx = gm_input_data(task, "bilinear", chunk=1600)
xx

# %%
# input_rgb = to_rgba(xx, clamp=(1, 3_000), bands=RGB)
# input_rgb.isel(spec=0).compute().plot.imshow(size=6, aspect=1)
# input_rgb.isel(spec=np.s_[:9]).compute().plot.imshow(col='spec', col_wrap=3, size=6, aspect=1)

# %%
gm = gm_reduce(xx)
gm

# %%
gm = gm.isel(y=np.s_[-1600:])

# %%
# %%time
_gm = gm.compute()

# %%
_gm_rgba = to_rgba(_gm, clamp=(1, 3_000), bands=RGB)

# %%
show_im(_gm_rgba.data)
