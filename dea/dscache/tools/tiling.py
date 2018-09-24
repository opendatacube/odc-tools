from types import SimpleNamespace
import toolz


def web_gs(zoom, tile_size=256):
    """ Construct grid spec compatible with TerriaJS requests at a given level.

    Tile indexes should be the same as google maps, except that Y component is negative,
    this is a limitation of GridSpec class, you can not have tile index direction be
    different from axis direction, but this is what google indexing is using.

    http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/
    """
    from datacube.utils.geometry import CRS
    from datacube.model import GridSpec
    from math import pi

    R = 6378137

    origin = pi * R
    res0 = 2 * pi * R / tile_size
    res = res0*(2**(-zoom))
    tsz = 2 * pi * R * (2**(-zoom))  # res*tile_size

    return GridSpec(crs=CRS('epsg:3857'),
                    tile_size=(tsz, tsz),
                    resolution=(-res, res),        # Y,X
                    origin=(origin-tsz, -origin))  # Y,X


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
