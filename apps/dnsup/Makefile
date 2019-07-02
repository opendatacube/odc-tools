all: dea-dnsup.xar

./_xar/bin/pip:
	mkdir -p _xar
	python3 -m venv _xar

_xar_env: ./_xar/bin/pip
	$< install git+https://github.com/facebookincubator/xar.git@e80d9ede6767f4c06f478c1b3f0bb3b4cb50072d
	$< install .

dea-dnsup.xar: _xar_env setup.py
	./_xar/bin/python3 setup.py bdist_xar --xar-compression-algorithm=zstd --dist-dir=.

install:
	install -D -m 755 dea-dnsup.xar /usr/bin/dea-dnsup.xar
	install -D -m 644 dea-dns.service /lib/systemd/system/dea-dns.service
	install -D -m 755 dea-dns /lib/systemd/system-sleep/dea-dns
	@echo "Run: 'sudo systemctl enable dea-dns' to enable it"

clean:
	rm -rf _xar build dea_dnsup.egg-info

distclean: clean
	rm -f dea-dnsup.xar


.PHONY: all clean distclean
