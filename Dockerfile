FROM ekidd/rust-musl-builder:stable
MAINTAINER dev@ionchannel.io

ARG APP_NAME
ARG BUILD_DATE
ARG VERSION
ARG GIT_COMMIT_HASH
ARG ENVIRONMENT

WORKDIR /home/rust/src
COPY . .

LABEL org.metadata.build-date=$BUILD_DATE \
      org.metadata.version=$VERSION \
      org.metadata.vcs-url="https://github.com/ion-channel/rusty-von-humbolt" \
      org.metadata.vcs-commit-id=$GIT_COMMIT_HASH \
      org.metadata.name="RvH" \
      org.metadata.description="Ion Channel GA Data Ingestor"

RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:3.10
WORKDIR /usr/src/rusty-von-humboldt
COPY --from=0 /home/rust/src/target/x86_64-unknown-linux-musl/release/rusty-von-humboldt ./rusty-von-humboldt
CMD /usr/src/rusty-von-humboldt/rusty-von-humboldt
