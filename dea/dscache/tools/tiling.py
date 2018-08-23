from types import SimpleNamespace


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

        for tile, geobox in gridspec.tiles_inside_geopolygon(ds.extent, geobox_cache=geobox_cache):
            register(tile, geobox, ds_val)

    return cells
