FROM ubuntu:bionic-20191029

# For deployment, we need
# libc6 - dynamically linked by znbase binary
# ca-certificates - to authenticate TLS connections for telemetry and
#                   bulk-io with S3/GCS/Azure
# tzdata - for time zone functions
RUN apt-get update && \
	apt-get -y upgrade && \
	apt-get install -y libc6 ca-certificates tzdata && \
	rm -rf /var/lib/apt/lists/*

RUN mkdir -p /znbase

COPY znbase.sh znbase /znbase/
# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /znbase/

ENV ZNBASE_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/znbase/znbase.sh"]
