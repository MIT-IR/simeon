# Dockerfile for running simeon in a container
FROM python:3.11-slim
# Set a key-value label for the Docker image
LABEL maintainer="MIT Institutional Research"
LABEL email="irx@mit.edu"

# Create the home directory
RUN mkdir /simeon

# Create the simeon user and group
RUN useradd -d /simeon -r -U simeon
RUN chown simeon:simeon /simeon

#  Define the working directory within the container
WORKDIR /simeon

# Add the requirements.txt file to start building the package
ADD requirements.txt /simeon/build_dir/requiremens.txt

# Update the repositories used by apt and install gpg
RUN apt-get update -y
RUN apt-get install -y gnupg2

# Install python packages needed by simeon
RUN pip install -U pip wheel

# Install simeon with geoip support
ADD . /simeon/build_dir/

# Clean up
RUN pip install /simeon/build_dir[geoip]
RUN rm -rf /simeon/*

# Set the user to simeon
USER simeon:simeon

# Start a shell
CMD [ "bash" ]
