# Base image to use for constructing environment
V_BASE ?= 3.3.0

# Docker we are building
DKR_IMG ?= opendatacube/odc-test-runner:latest

# Absolute path for this directory
WK := $(shell pwd)

# Absolute path to code
CODE := $(shell readlink -f ..)
TTY := $(shell bash -c "tty -s && echo '-t' || true")

dkr := docker run --rm -i $(TTY) \
        -v $(CODE):/code \
        -v $(WK):/wk \
        -e TZ=Australia/Sydney \
        $(BUILDER_IMG)

all: dkr

bash:
	@docker run --rm -ti \
    -v $(CODE):/code \
    -v $(WK):/wk \
    $(DKR_IMG) bash

constraints.txt: requirements.txt
	@if [ -e /.dockerenv ]; then \
      pip-compile -v \
         --no-annotate \
         --strip-extras \
         --no-build-isolation \
         --cache-dir .cache \
         --output-file $@ $< ; \
   else \
      docker run --rm \
       -v $(CODE):/code \
       $(DKR_IMG) \
       make -C docker $@; \
   fi

dkr: Dockerfile
	DOCKER_BUILDKIT=1 docker build \
  --build-arg V_BASE=$(V_BASE) \
  -t $(DKR_IMG) \
  -f Dockerfile .

dkr-no-deps:
	DOCKER_BUILDKIT=1 docker build \
  --build-arg V_BASE=$(V_BASE) \
  -t $(DKR_IMG) \
  -f Dockerfile .

run-test:
	@docker run --rm $(TTY) \
    -v $(CODE):/code \
    $(DKR_IMG) with-test-db env AWS_DEFAULT_REGION=us-west-2 DASK_TEMPORARY_DIRECTORY=/tmp/dask \
               pytest --cov=. \
               --cov-report=html \
               --cov-report=xml:coverage.xml \
               --timeout=30 \
               libs apps

clean:
	@echo "not implemented"

.PHONY: dbg all clean dkr run-test dkr dkr-no-deps bash
