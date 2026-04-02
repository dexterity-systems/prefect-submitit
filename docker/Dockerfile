FROM nathanhess/slurm:full-v1.2.0

USER root

# Install pixi (manages Python, PostgreSQL, and all project deps)
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_HOME=/usr/local bash

# Store pixi envs on a separate volume (/pixi-env) so they don't
# clash with the host's macOS .pixi/ from the bind mount.
RUN pixi config set detached-environments /pixi-env --system

# MariaDB for SLURM accounting storage (sacct support)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        mariadb-server slurmdbd && \
    rm -rf /var/lib/apt/lists/*

# slurmdbd configuration (contains DB password -- restrict permissions)
COPY slurmdbd.conf /etc/slurm/slurmdbd.conf
RUN chown slurm:slurm /etc/slurm/slurmdbd.conf && \
    chmod 600 /etc/slurm/slurmdbd.conf

# PostgreSQL unix socket directory permissions
RUN mkdir -p /var/run/postgresql && chmod 1777 /var/run/postgresql

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Entrypoint runs as root (for SLURM daemons and volume permissions),
# then drops to the docker user for the main process.
WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
