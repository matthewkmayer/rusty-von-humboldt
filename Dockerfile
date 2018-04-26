FROM phusion/baseimage
MAINTAINER dev@ionchannel.io

ARG APP_NAME
ARG BUILD_DATE
ARG VERSION
ARG GIT_COMMIT_HASH
ARG ENVIRONMENT

RUN apt-get update
RUN apt-get install g++ openssl pkg-config libssl-dev -y
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y && $HOME/.cargo/bin/rustc --version

WORKDIR /usr/src/rusty-von-humboldt
COPY . .

LABEL org.metadata.build-date=$BUILD_DATE \
      org.metadata.version=$VERSION \
      org.metadata.vcs-url="https://github.com/ion-channel/rusty-von-humbolt" \
      org.metadata.vcs-commit-id=$GIT_COMMIT_HASH \
      org.metadata.name="RvH" \
      org.metadata.description="Ion Channel GA Data Ingestor"

RUN $HOME/.cargo/bin/cargo install

CMD /root/.cargo/bin/rusty-von-humboldt
