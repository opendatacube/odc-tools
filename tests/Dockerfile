FROM opendatacube/geobase:wheels as env_builder

ARG py_env_path=/env

ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

ADD ./tests/requirements-apt.txt /tmp/
RUN apt-get update \
    && sed 's/#.*//' /tmp/requirements-apt.txt | xargs apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Install our requirements
RUN mkdir -p /conf
COPY ./tests/requirements.txt /conf/
RUN env-build-tool new /conf/requirements.txt ${py_env_path} /wheels

# Install the tools
ADD ./ /code
WORKDIR /code

# This installs things twice because of circular dependencies
RUN /code/scripts/dev-install.sh

# Copy in the test runner script
COPY ./tests/run_tests.sh /usr/local/bin/run_tests.sh
ENV PATH="/env/bin:${PATH}"
