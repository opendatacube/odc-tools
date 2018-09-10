from types import SimpleNamespace
import toolz


def extract_ls_path_row(ds):
    full_id = ds.metadata_doc.get('tile_id')

    if full_id is None:
        full_id = toolz.get_in(['usgs', 'scene_id'], ds.metadata_doc)

    if full_id is None:
        return None
    return tuple(int(s) for s in (full_id[3:6], full_id[6:9]))


def bin_dataset_stream(gridspec, dss, persist=None):
    """

    :param gridspec: GridSpec
    :param dss: Sequence of datasets (can be lazy)
    :param persist: Dataset -> SomeThing mapping, defaults to keeping dataset id only
    """
    cells = {}
    geobox_cache = {}

    def default_persist(ds):
        return ds.id

    def register(tile, geobox, val):
        cell = cells.get(tile)
        if cell is None:
            cells[tile] = SimpleNamespace(geobox=geobox, idx=tile, dss=[val])
        else:
            cell.dss.append(val)

    if persist is None:
        persist = default_persist

    for ds in dss:
        ds_val = persist(ds)

        if ds.extent is None:
            print('WARNING: Datasets without extent info: %s' % str(ds.id))
            continue

        for tile, geobox in gridspec.tiles_from_geopolygon(ds.extent, geobox_cache=geobox_cache):
            register(tile, geobox, ds_val)

    return cells


def bin_by_native_tile(dss, persist=None, native_tile_id=None):
    """Group datasets by native tiling, like path/row for Landsat.

    :param dss: Sequence of datasets (can be lazy)

    :param persist: Dataset -> SomeThing mapping, defaults to keeping dataset
    id only

    :param native_tile_id: Dataset -> Key, defaults to extracting `path,row`
    tuple from metadata's `tile_id` field, but could be anything, only
    constraint is that Key value can be used as index to python dict.
    """

    cells = {}

    def default_persist(ds):
        return ds.id

    def register(tile, val):
        cell = cells.get(tile)
        if cell is None:
            cells[tile] = SimpleNamespace(idx=tile, dss=[val])
        else:
            cell.dss.append(val)

    native_tile_id = native_tile_id or extract_ls_path_row
    persist = persist or default_persist

    for ds in dss:
        ds_val = persist(ds)
        tile = native_tile_id(ds)
        if tile is None:
            raise ValueError('Missing tile id')
        else:
            register(tile, ds_val)

    return cells
