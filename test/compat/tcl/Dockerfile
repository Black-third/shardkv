# Redis's own test suite, in external mode.
#
# This is the highest-authority signal available to a Redis-compatible server:
# tests written by the people who define the behaviour, asserting on the exact
# replies and the exact error strings, with no idea that they are not talking to
# Redis. Nothing written in this repository can be as good a witness.
#
# The version is pinned rather than tracking unstable, and it is pinned to the
# release whose behaviour shardkv claims to implement. Running unstable's suite
# would measure the distance to a moving target and report it as failure.
FROM alpine:3.21

ARG REDIS_VERSION=7.4.2

RUN apk add --no-cache tcl curl bash procps \
    && curl -fsSL "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz" \
    | tar xz -C /opt \
    && mv "/opt/redis-${REDIS_VERSION}" /opt/redis

ENV REDIS_VERSION=${REDIS_VERSION}
COPY run.sh /run.sh
CMD ["/run.sh"]
