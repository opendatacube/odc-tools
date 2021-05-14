# Base image to use for constructing environment
BUILDER_IMG ?= opendatacube/geobase-builder:3.3.0

# Docker we are building
DKR_IMG ?= opendatacube/odc-test-runner:latest

# Absolute path for this directory
WK := $(shell pwd)

# Absolute path to code
CODE := $(shell readlink -f ..)

# ENV -- location of python env inside the docker
ENV := "/env"

dkr := docker run --rm -i \
        -v $(CODE):/code \
        -v $(WK):/wk \
        -v $(WK)/.build/env:$(ENV) \
        -e TZ=Australia/Sydney \
        -e NOBINARY=/wk/nobinary.txt \
        $(BUILDER_IMG)

all: dkr

download: .build/download.info.txt
compile: .build/compile.info.txt
env: .build/env.info.txt

env.tgz: .cache/env.info.txt
	@$(dkr) tar czf $@ $(ENV)

.build/prepared.txt:
	@mkdir -p .cache/pip .build/env
	@date > $@

.build/download.info.txt: requirements.txt constraints.txt nobinary.txt .build/prepared.txt
	@$(dkr) env-build-tool download requirements.txt constraints.txt ./wheels --find-links ./wheels
	@date > $@

.build/compile.info.txt: .build/download.info.txt
	@$(dkr) env-build-tool compile ./wheels
	@date > $@

.build/env.info.txt: .build/compile.info.txt
	@$(dkr) env-build-tool new_no_index rr-odc-tools.in constraints.txt $(ENV) ./wheels
	@date > $@

bash: .build/prepared.txt
	@$(dkr) bash

bash-runner:
	@docker run --rm -ti \
    -v $(CODE):/code \
    -v $(WK):/wk \
    $(DKR_IMG) bash

dbg: .build/prepared.txt
	@echo "dkr: " $(dkr)
	@$(dkr) python --version

dkr: .build/env.info.txt Dockerfile
	docker build -t $(DKR_IMG) --cache-from $(DKR_IMG) .

dkr-no-deps:
	docker build -t $(DKR_IMG) --cache-from $(DKR_IMG) .

run-test:
	@docker run --rm -i \
    -v $(CODE):/code \
    $(DKR_IMG) pytest .

clean:
	rm -f .build/download.info.txt .build/compile.info.txt .build/env.info.txt
	rm -rf .build/env
	@echo "Keeping wheels and pip cache"

.PHONY: dbg all clean download compile env dkr
