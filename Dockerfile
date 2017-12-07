FROM phusion/baseimage

RUN apt-get update
RUN apt-get install g++ openssl pkg-config libssl-dev -y
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

RUN $HOME/.cargo/bin/rustc --version

WORKDIR /usr/src/rusty-von-humboldt
COPY . .

# Switch this to --release when we're doing it for reals
RUN $HOME/.cargo/bin/cargo install

# Use an env file instead? Less rebuilding of things.
CMD GHABUCKET=foo /root/.cargo/bin/rusty-von-humboldt