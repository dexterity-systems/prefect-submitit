#!/bin/bash
set -e

# --- Configure SLURM (replicate /etc/startup.sh sed commands) ---
sed -i "s/<<HOSTNAME>>/$(hostname)/" /etc/slurm/slurm.conf
sed -i "s/<<CPU>>/$(nproc)/" /etc/slurm/slurm.conf
REAL_MEM=$(slurmd -C | grep -oP 'RealMemory=\K[0-9]+')
sed -i "s/<<MEMORY>>/$REAL_MEM/" /etc/slurm/slurm.conf

# Enable accounting storage (replaces accounting_storage/none)
sed -i 's|AccountingStorageType=accounting_storage/none|AccountingStorageType=accounting_storage/slurmdbd|' /etc/slurm/slurm.conf
sed -i '/AccountingStorageType/a AccountingStorageHost=localhost' /etc/slurm/slurm.conf

# --- Start daemons in dependency order ---

# 1. Munge (auth for all SLURM daemons including slurmdbd)
service munge start

# 2. MariaDB (socket-only, no TCP)
mysqld_safe --skip-networking &
for i in $(seq 1 30); do
    mysqladmin ping --silent 2>/dev/null && break
    [ "$i" -eq 30 ] && { echo "ERROR: MariaDB did not start." >&2; exit 1; }
    sleep 1
done

# 3. Initialize accounting database (idempotent)
mysql -e "CREATE DATABASE IF NOT EXISTS slurm_acct_db;"
mysql -e "CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'slurm';"
mysql -e "GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost';"

# 4. slurmdbd
slurmdbd
for i in $(seq 1 30); do
    sacctmgr show cluster --noheader --parsable2 2>/dev/null && break
    [ "$i" -eq 30 ] && { echo "ERROR: slurmdbd did not start." >&2; exit 1; }
    sleep 1
done

# 5. Register cluster and default account (idempotent -- errors ignored)
sacctmgr -i add cluster cluster 2>/dev/null || true
sacctmgr -i add account default cluster=cluster 2>/dev/null || true
sacctmgr -i add user docker account=default 2>/dev/null || true

# 6. SLURM compute and controller daemons
service slurmd start
service slurmctld start

# Wait for SLURM controller to accept commands
for i in $(seq 1 30); do
    if squeue --noheader 2>/dev/null; then
        echo "SLURM is ready."
        break
    fi
    [ "$i" -eq 30 ] && { echo "ERROR: SLURM did not become ready in 30s." >&2; exit 1; }
    sleep 1
done

# Fix ownership of the pixi env volume (Docker creates named volumes as root)
chown -R docker:docker /pixi-env 2>/dev/null || true

# Drop to non-root user for the main process
exec runuser -u docker -- "$@"
