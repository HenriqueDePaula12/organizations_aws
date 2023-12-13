ARG PYTHON_VERSION=3.10.6

FROM --platform=linux/amd64 amazonlinux:2 AS base
ARG PYTHON_VERSION

RUN yum install -y gcc openssl11-devel bzip2-devel libffi-devel tar gzip wget make && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations && \
    make install
   
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV --copies
RUN cp -r /usr/local/lib/python3.10/* $VIRTUAL_ENV/lib/python3.10/

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install install \
    requests==2.22.0 \
    boto3==1.28.83 \
    venv-pack==0.2.0 

RUN mkdir /output && \
    venv-pack -o /output/pyspark_${PYTHON_VERSION}.tar.gz --python-prefix /home/hadoop/environment

FROM scratch AS export
ARG PYTHON_VERSION
COPY --from=base /output/pyspark_${PYTHON_VERSION}.tar.gz /pyspark_python_extra_libs.tar.gz