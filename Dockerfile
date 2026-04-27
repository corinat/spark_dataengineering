# Use Eclipse Temurin JDK 17 on Ubuntu Jammy as the base image
FROM eclipse-temurin:17-jdk-jammy

# Install GDAL, Python build deps (for pyenv), and other tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    curl \
    wget \
    git \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncursesw5-dev \
    xz-utils \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# create a non-root user
RUN useradd -ms /bin/bash myuser

# switch to non-root user for application setup
USER myuser

# set environment variables for the user
ENV HOME="/home/myuser" \
    PYENV_ROOT="/home/myuser/.pyenv"

# install pyenv under the non-root user
RUN curl https://pyenv.run | bash

# add pyenv to the PATH for the non-root user
ENV PATH="/home/myuser/.pyenv/bin:/home/myuser/.pyenv/shims:$PATH"
RUN echo 'export PATH="/home/myuser/.pyenv/bin:$PATH"' >> /home/myuser/.bashrc
RUN echo 'export PATH="/home/myuser/.pyenv/shims:$PATH"' >> /home/myuser/.bashrc
RUN echo 'eval "$(pyenv init --path)"' >> /home/myuser/.bashrc
RUN echo 'eval "$(pyenv init -)"' >> /home/myuser/.bashrc

# install python version using pyenv
RUN pyenv install 3.11.4 && pyenv global 3.11.4

# upgrade pip to latest version
RUN pip install --upgrade pip

# install poetry under the non-root user
RUN curl -sSL https://install.python-poetry.org | python3 -

# ensure poetry is available in the PATH for the non-root user
ENV PATH="/home/myuser/.local/bin:$PATH"

WORKDIR /app

COPY --chown=myuser:myuser ./pyproject.toml /app/pyproject.toml
COPY --chown=myuser:myuser . /app

# use poetry to create the virtual environment after copying the necessary files
RUN poetry env use "/home/myuser/.pyenv/versions/3.11.4/bin/python3"

# install dependencies using poetry (regenerates lock file then installs)
RUN rm -f poetry.lock && poetry lock && poetry install

# download Sedona JARs into Spark's jars folder
RUN SPARK_JARS=$(poetry run python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    wget -q -P "$SPARK_JARS" https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-4.1_2.13/1.9.0/sedona-spark-shaded-4.1_2.13-1.9.0.jar && \
    wget -q -P "$SPARK_JARS" https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.9.0-33.5/geotools-wrapper-1.9.0-33.5.jar

# verify setup
RUN poetry run python tests/test_sedona.py
RUN poetry run python tests/test_geotools.py

RUN poetry run pytest tests/unit

RUN poetry run pytest tests/integration

RUN poetry run mypy --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs \
    data_transformations tests

RUN poetry run pylint --fail-under=9.0 data_transformations tests