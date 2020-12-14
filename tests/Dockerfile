FROM opendatacube/geobase:builder-3.1.3 as env_builder

ARG py_env_path=/env

ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# if using binary rasterio package, to support https
RUN mkdir -p /etc/pki/tls/certs \
  && ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt;

# Install our requirements
RUN mkdir -p /conf
COPY ./tests/requirements.txt /conf/
COPY ./tests/constraints.txt /conf/
RUN python3 -m venv /env \
  && /env/bin/python -m pip install -U pip wheel setuptools -c /conf/constraints.txt \
  && /env/bin/python -m pip install -r /conf/requirements.txt -c /conf/constraints.txt \
  && /env/bin/python -m pip install -r /conf/constraints.txt \
  && rm -rf /root/.cache/pip \
  && echo done

# Install the tools
ADD ./ /code
WORKDIR /code

ENV PATH="/env/bin:${PATH}"
RUN ./scripts/dev-install.sh --extra-index-url=https://packages.dea.ga.gov.au/ -c /conf/constraints.txt

# Copy in the test runner script
COPY ./tests/run_tests.sh /usr/local/bin/run_tests.sh
