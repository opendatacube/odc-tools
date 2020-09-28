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
def dump_mem_needs(nx, ny=None, nt=150, nb=10, work_rows=64):
    if ny is None:
        ny = nx

    GB = 1<<30
    N = nx*ny*nt
    N_ = work_rows*nx*nt

    print(f"""Input:
  {ny}x{nx}x{nb}x{nt}
Bytes:
  u16      : {N*nb*2/GB:.2f}Gb
  f32      : {N*nb*4/GB:.2f}Gb
  u16+f32  : {N*nb*6/GB:.2f}Gb

  f32'     : {N_*nb*4/GB:.2f}Gb
  u16+f32' : {(N*nb*2 + N_*nb*4)/GB:.2f}Gb

  madw     : {N*4/GB:.2f}Gb
  temp.mask: {N*4/GB:.2f}Gb
...""")


# %%
dump_mem_needs(800, nt=74)

# %%
for n in range(1, 4+1):
    dump_mem_needs(n*800)

# %%
