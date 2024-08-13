FROM alpine:3.18
RUN apk update && apk add --no-cache wget ca-certificates bash findutils

RUN cd /usr/local/bin && \
    wget -q --show-progress https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc
