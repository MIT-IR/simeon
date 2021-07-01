# Dockerfile for running simeon in a container
FROM python:3.8-slim
# set a key-value label for the Docker image
LABEL maintainer="MIT Institutional Research"
LABEL email="irx@mit.edu"
COPY . /simeon
#  defines the working directory within the container
WORKDIR /simeon
# Update the repositories used by apt
RUN apt-get update -y
# Install python packages needed by simeon
RUN pip install -U wheel
# Install simeon with geoip support
RUN pip install .[geoip]
# Start a shell
CMD [ "bash" ]
