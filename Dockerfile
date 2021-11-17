FROM  ubuntu:18.04
#FROM gcr.io/distroless/static:latest
LABEL maintainers="DELL EMC ObjectScale"
LABEL description="ObjectScale S3 Client"

COPY ./s3cli s3cli
ENTRYPOINT ["/s3cli"]
