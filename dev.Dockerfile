FROM debian:11

RUN \
    echo " \
        APT::Install-Suggests false; \
        APT::Install-Recommends false; \
    " >>/etc/apt/apt.conf.d/no-suggests-recommends \
    \
    && apt update \
    \
    && apt install -y \
      openssh-server openssh-client sudo \
      procps htop vim curl bash-completion \
      leiningen

RUN \
    mkdir /run/sshd \
    \
    && useradd -m -G sudo kda

ENTRYPOINT ["/usr/sbin/sshd", "-D"]
