#syntax=docker/dockerfile:1.2
ARG V_PG=14
ARG V_PGIS=14-postgis-3

FROM osgeo/gdal:ubuntu-small-3.5.3
ENV LC_ALL=C.UTF-8
ENV PATH="/env/bin:${PATH}"

USER root
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends --allow-change-held-packages \
    # git is needed for sdist|bdist_wheel \
    python3-dev python3-distutils python3-pip \
    git \
    # for docs
    build-essential \
    graphviz \
    # for integration tests \
    libpq-dev libproj-dev \
    postgresql \
    postgresql-client-${V_PG} \
    postgresql-${V_PG} \
    postgresql-${V_PGIS} \
    # for matching directory permissions when running tests as non-root user \
    tini \
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 odc \
  && useradd --gid 1000 \
  --uid 1000 \
  --create-home \
  --shell /bin/bash -N odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && install -d -o odc -g odc /env \
  && install -d -o odc -g odc /code \
  && install -d -o odc -g odc -D /var/run/postgresql /srv/postgresql \
  && install -d -o odc -g odc -D /home/odc/.cache/pip \
  && chown -R odc:odc /home/odc/ \
  && true

COPY constraints.txt requirements.in /conf/

RUN python3 -m pip install -U pip cython

RUN python3 -m pip install \
       -r /conf/requirements.in \
       -c /conf/constraints.txt


# Bake in fresh empty datacube db into docker image (owned by odc user)
# Need to run after environment setup as it needs datacube
COPY --chown=0:0 assets/with-bootstrap /usr/local/bin/
COPY --chown=0:0 assets/with-test-db /usr/local/bin/

USER odc
RUN with-bootstrap with-test-db prepare

USER root

WORKDIR /code
ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with-bootstrap"]
