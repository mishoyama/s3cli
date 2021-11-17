FROM  ubuntu:18.04
LABEL maintainers="DELL EMC ObjectScale"
LABEL description="ObjectScale S3 Client"

COPY ./s3cli /usr/bin/s3cli

ADD  ./entrypoint.sh entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD        ["demon"]
