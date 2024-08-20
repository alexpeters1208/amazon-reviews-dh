FROM ghcr.io/deephaven/server:latest

# copy python requirements and data
COPY requirements.txt /requirements.txt

# install python requirements
RUN pip install -r /requirements.txt && rm /requirements.txt
