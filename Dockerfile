FROM  ubuntu:18.04
LABEL maintainers="DELL EMC ObjectScale"
LABEL description="ObjectScale S3 Client"

COPY ./s3cli /usr/bin/s3cli

ADD  ./entrypoint.sh entrypoint.sh

ADD  ./1024px-Dell_Logo.svg.png image.png

ADD ./store_data.sh push_image

ADD ./bucket_parser /bucket_parser

ENTRYPOINT ["/entrypoint.sh"]
CMD        ["demon"]
