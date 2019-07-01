all: dea-dnsup.xar

./_xar/bin/pip3:
	mkdir -p _xar
	python3 -m venv _xar

_xar_env: ./_xar/bin/pip3
	$< install git+https://github.com/facebookincubator/xar.git@e80d9ede6767f4c06f478c1b3f0bb3b4cb50072d
	$< install .

dea-dnsup.xar: _xar_env setup.py
	./_xar/bin/python3 setup.py bdist_xar --xar-compression-algorithm=zstd --dist-dir=.

clean:
	rm -rf _xar build dea_dnsup.egg-info

distclean: clean
	rm -f dea-dnsup.xar


.PHONY: all clean distclean _xar_env
