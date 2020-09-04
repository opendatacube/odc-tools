import pandas as pd
from tqdm.auto import tqdm
from typing import Dict, Tuple, List
from collections import namedtuple
from datetime import datetime
from datacube.utils.dates import normalise_dt

from .model import DateTimeRange

CompressedDataset = namedtuple("CompressedDataset", ['id', 'time'])

def find_seasonal_bin(dt: datetime, freq: int, anchor: int) -> DateTimeRange:
        dtr = DateTimeRange(f"{dt.year}-{anchor}--P{freq}")
        if dt in dtr:
            return dtr
        step = 1 if dt > dtr else -1
        while dt not in dtr:
            dtr = dtr + step
        return dtr

def bin_data(cells: Dict[Tuple[int, int], List[CompressedDataset]], frequency) \
             -> Dict[Tuple[str, int, int], List[CompressedDataset]]:
        ids = []
        timestamps = []
        tidxs = []
        datasets = []

        # Read all the data
        for tidx, cell in cells.items():
            ids.extend([dss.id for dss in cell.dss])
            timestamps.extend([normalise_dt(dss.time) for dss in cell.dss])
            datasets.extend(dss for dss in cell.dss)
            tidxs.extend([tidx] * len(cell.dss))

        # Find min and max timestamps in the dataset(s)
        start = min(timestamps)
        end = max(timestamps)

        # Put all the data in one bin
        if frequency == 'all':
            bin = (f"{start.year}--P{end.year - start.year + 1}Y",)
            tasks = {bin + tidxs[0]: datasets}

        # Seasonal binning
        # 1900 is a dummy start year for the case where start year is %Y
        elif frequency is not None and frequency.start.year == 1900:
            tasks = {}
            anchor = frequency.start.month
            df = pd.DataFrame({"id": ids, "timestamp": timestamps, "tidx": tidxs, 'ds': datasets})
            for dt in tqdm(timestamps[0:10]):
                df['bin'] = find_seasonal_bin(dt, frequency.freq, anchor).short

            for k, v in df.groupby(['tidx', "bin"]):
                tasks[(k[1],)+ k[0]] = v['ds']

        else:
            # Annual binning, this is the default
            tasks = {}
            for tidx, cell in cells.items():
                # TODO: deal with UTC offsets for day boundary determination
                grouped = toolz.groupby(lambda ds: ds.time.year, cell.dss)
                for year, dss in grouped.items():
                    temporal_k = (f"{year}--P1Y",)
                    tasks[temporal_k + tidx] = dss
        return tasks