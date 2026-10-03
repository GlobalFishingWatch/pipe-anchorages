# ---------------------------------------------------------------------------------------
# BUILDER
# ---------------------------------------------------------------------------------------
# Pinned explicitly (rather than left to float) so that the GDAL/Fiona C extension
# compiled here in `builder` and the runtime libgdal installed separately in `prod` are
# always the exact same build -- the two stages each run their own apt-get, so nothing
# else guarantees they'd match. See GlobalFishingWatch/pipe-regions' Dockerfile for the
# same pattern (there, pinned because a direct `gdal` PyPI dependency needs it; here,
# because Fiona<2 compiles its own extension against libgdal at install time).
ARG GDAL_APT_VERSION=3.6.2+dfsg-1+b2

FROM python:3.12-slim-bookworm AS builder
ARG GDAL_APT_VERSION

VOLUME ["/root/.config"]

# Build tools + GDAL headers: Fiona<2 compiles its C extension against libgdal at
# install time (no manylinux wheels for this major version), so these aren't optional.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libexpat1 \
        gdal-bin=${GDAL_APT_VERSION} libgdal-dev=${GDAL_APT_VERSION} \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for GDAL headers (useful if installing Python bindings via pip later)
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Use uv for high-speed installs
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1

# Install dependencies BEFORE copying source so an edit under src/
# doesn't invalidate the cache of the (expensive) requirements-install layer.
COPY pyproject.toml requirements.txt README.md MANIFEST.in ./
RUN uv pip install --system --upgrade pip && \
    uv pip install --system build && \
    uv pip install --system --prefix=/install -r requirements.txt

COPY src ./src
RUN uv pip install --system --prefix=/install --no-deps .

# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS prod
ARG GDAL_APT_VERSION

ENV PYTHONUNBUFFERED=1

# Fiona's compiled extension links against libgdal at runtime, so the final image needs
# the matching GDAL runtime packages -- copying /install alone (as a repo with no
# native-extension deps could) isn't enough here. No compile toolchain needed at this
# point though (gcc/g++/build-essential stay in `builder`, not copied here).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libexpat1 \
        gdal-bin=${GDAL_APT_VERSION} libgdal-dev=${GDAL_APT_VERSION} \
    && rm -rf /var/lib/apt/lists/*

# COPY PYTHON PACKAGES
COPY --from=builder /install /usr/local

# APACHE BEAM INTEGRATION
# IMPORTANT: this version must match apache-beam[gcp] in pyproject.toml.
COPY --from=apache/beam_python3.12_sdk:2.69.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT ["/opt/apache/beam/boot"]

WORKDIR /opt/project

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE
# ---------------------------------------------------------------------------------------
FROM builder AS dev

RUN apt-get update && \
    apt-get install -y --no-install-recommends make && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY . .
RUN uv pip install --system -e .[lint,dev,build] && \
    uv pip install --system -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./requirements-test.txt .
RUN pip install -r requirements-test.txt

COPY ./tests ./tests

# Suppress all warnings during tests
# To see/address warnings, run tests in your development environment.
ENV PYTHONWARNINGS=ignore
