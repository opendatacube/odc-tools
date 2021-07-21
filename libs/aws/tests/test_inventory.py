import time

from odc.aws.inventory import list_inventory


def time_process(start: float):
    """
    Times the process
    :param start:
    :return:
    """
    t_sec = round(time.time() - start)
    (t_min, t_sec) = divmod(t_sec, 60)
    (t_hour, t_min) = divmod(t_min, 60)

    return f"{t_hour} hour: {t_min} min: {t_sec} sec"


def test_inventory():
    start_per_msg = time.time()
    list_assets = list_inventory(
        manifest='s3://deafrica-landsat-inventory/deafrica-landsat/deafrica-landsat-inventory/'
    )

    print(f'first 10 {[key for key in list_assets][0:10]}')

    print(
        f"Processed in {time_process(start=start_per_msg)}"
    )


def test_inventory_threads():
    start_per_msg = time.time()
    list_assets = list_inventory(
        manifest='s3://deafrica-landsat-inventory/deafrica-landsat/deafrica-landsat-inventory/',
        n_threads=200
    )

    print(f'first 10 {[key for key in list_assets][0:10]}')

    print(
        f"Processed in {time_process(start=start_per_msg)}"
    )
