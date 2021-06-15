# Dockerfile for running simeon in a container
FROM python:3.8
# set a key-value label for the Docker image
LABEL maintainer="MIT Insitutional Research"
COPY . /simeon
#  defines the working directory within the container
WORKDIR /simeon
RUN pip install -U wheel
RUN pip install -r requirements.txt
RUN pip install .[geoip]
CMD [ "bash" ]
