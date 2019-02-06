FROM alpine:latest

LABEL maintainer "Steven Armstrong <steven.armstrong@id.ethz.ch>"

RUN apk --no-cache -X "@edge http://dl-cdn.alpinelinux.org/alpine/edge/main" \
   add --upgrade apk-tools@edge \
   zfs@edge

COPY ./local-zfs-provisioner /local-zfs-provisioner

ENTRYPOINT ["/local-zfs-provisioner"]
