FROM  ubuntu:18.04
LABEL maintainers="DELL EMC ObjectScale"
LABEL description="ObjectScale S3 Client"

COPY ./s3cli /usr/bin/s3cli

RUN apt-get update && apt-get install -y nginx

CMD ["nginx", "-g", "daemon off;"]
