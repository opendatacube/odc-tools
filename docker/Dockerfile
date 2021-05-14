FROM opendatacube/geobase-runner:3.3.0
ENV LC_ALL=C.UTF-8
ENV PATH="/env/bin:${PATH}"


RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends --allow-change-held-packages \
    # to become test user
    sudo \
    # git is needed for sdist|bdist_wheel
    git \
    # for docs
    make \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 odc \
    && useradd --gid 1000 \
    --uid 1000 \
    --create-home \
    --shell /bin/bash -N odc \
    && adduser odc users \
    && adduser odc sudo \
    && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && true


COPY with_bootstrap /usr/local/bin
ENTRYPOINT ["/usr/local/bin/with_bootstrap"]
VOLUME ["/code"]
WORKDIR /code

# Environment contains odc-tools installed in edit mode in /code folder
ADD .build/env /env
