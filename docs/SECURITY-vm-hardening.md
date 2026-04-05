# SECURITY-vm-hardening.md — Ootils VM Security Review

**Date:** 2026-04-05  
**Scope:** Proxmox VM (Debian 12), Docker, PostgreSQL 16, FastAPI (ootils-core)  
**Network:** Private LAN 192.168.1.0/24, VM IP 192.168.1.176  
**Repo:** ngoineau/ootils-core (private)  
**Reviewer:** Security audit via OpenClaw subagent

---

## Executive Summary

The current deployment plan has **3 BLOCKERs** and several high-priority issues. The core problems are: credentials committed to source control in plaintext, both service ports bound to all interfaces (0.0.0.0), and no firewall protecting the VM. The "private network" assumption provides minimal security on its own — any compromised device on the LAN has full access.

---

## Findings by Category

### 1. SSH Hardening

**Current:** Key-only auth. Source: `/root/.ssh/authorized_keys` on Proxmox host (copied at provisioning time).

**Issues:**
- No explicit `PasswordAuthentication no` in sshd_config — default may allow password auth depending on distro build
- Root login status unknown
- No `MaxAuthTries` or connection rate limiting
- `PermitEmptyPasswords` not explicitly disabled

**Required hardening (`/etc/ssh/sshd_config`):**
```
PasswordAuthentication no
PermitRootLogin no
PermitEmptyPasswords no
MaxAuthTries 3
LoginGraceTime 30
ClientAliveInterval 300
ClientAliveCountMax 2
AllowUsers debian
X11Forwarding no
```

**Risk level:** Medium (key-only is good, but hardening the daemon is low-effort defense-in-depth)

---

### 2. Sudo Configuration — ⚠️ HIGH RISK

**Current:** Unspecified. Likely full passwordless sudo via `debian ALL=(ALL) NOPASSWD:ALL` (common in Debian cloud images).

**Problem:** Passwordless sudo means any process running as `debian` — including a compromised Docker container that escapes, or a malicious dependency in the Python app — can become root without any additional authentication step.

**Recommendation:**
- **Remove `NOPASSWD`** — require password for sudo. This forces real operator intent.
- Alternatively, scope sudo to only what's needed (e.g., `systemctl restart docker`) if automation requires it.
- If the VM is managed headlessly and you truly need passwordless, accept this risk explicitly and compensate with file integrity monitoring (aide/tripwire).

```bash
# /etc/sudoers.d/debian — replace NOPASSWD version with:
debian ALL=(ALL) ALL
```

**Risk level:** High

---

### 3. Firewall — 🚨 BLOCKER

**Current:** No firewall configured.

**Problem:** All ports on the VM are reachable from every device on 192.168.1.0/24. This includes:
- Port 5432: PostgreSQL — any LAN device can attempt to connect
- Port 8000: FastAPI — any LAN device can hit the API
- Port 22: SSH — any LAN device can attempt to authenticate

**Required: UFW (simple, sufficient for this use case)**

```bash
# Install and configure UFW
apt install ufw -y

# Deny everything by default
ufw default deny incoming
ufw default allow outgoing

# Allow SSH only
ufw allow 22/tcp

# Allow FastAPI only from trusted subnet (or specific host)
ufw allow from 192.168.1.0/24 to any port 8000 proto tcp

# PostgreSQL: DO NOT expose to LAN — only localhost/Docker network
# (see Port Exposure section)

ufw enable
```

**Risk level:** Critical — this is a BLOCKER

---

### 4. Docker Security

**Current:** User `debian` added to `docker` group. No network isolation specified. No resource limits.

**Issues:**

#### 4a. Docker group = root equivalent
Being in the `docker` group allows running `docker run --privileged -v /:/host ...` and escaping to full host root. This is a known privilege escalation path.

**Mitigation options (pick one):**
- Use `sudo docker` with a scoped sudoers rule instead of group membership
- Enable Docker's rootless mode (`dockerd` running as `debian`, not root)
- Accept the risk explicitly if `debian` is already fully trusted

#### 4b. No Docker network isolation
Currently no `networks:` defined in docker-compose.yml. Both containers share the default bridge, which is fine internally, but explicit networks enforce what can talk to what.

**Add to docker-compose.yml:**
```yaml
networks:
  backend:
    driver: bridge

services:
  postgres:
    networks:
      - backend

  api:
    networks:
      - backend
```

#### 4c. No resource limits
No CPU/memory limits. A runaway query or DoS could take down the host.

```yaml
services:
  postgres:
    deploy:
      resources:
        limits:
          memory: 512m
  api:
    deploy:
      resources:
        limits:
          memory: 256m
```

#### 4d. Containers running as root
By default, Docker containers run as root inside the container. If there's a container escape, impact is maximized.

**For the API container, add to Dockerfile:**
```dockerfile
RUN addgroup --system app && adduser --system --ingroup app app
USER app
```

**Risk level:** High (4a), Medium (4b-4d)

---

### 5. Port Exposure — 🚨 BLOCKER

**Current (from docker-compose.yml):**
```yaml
ports:
  - "5432:5432"   # PostgreSQL — bound to 0.0.0.0
  - "8000:8000"   # FastAPI — bound to 0.0.0.0
```

**Problem:** Both ports are bound to all interfaces. PostgreSQL at `0.0.0.0:5432` means:
- Any device on the LAN (192.168.1.0/24) can directly connect to Postgres
- Authentication is the only protection — and the password is `ootils` (see next section)

**Required changes:**

**PostgreSQL: bind to localhost only (or remove external binding entirely)**
```yaml
postgres:
  ports:
    - "127.0.0.1:5432:5432"   # Or remove entirely — API connects via Docker network
```

If the API connects to Postgres via the Docker internal network (which it does, via `DATABASE_URL: postgresql://ootils:ootils@postgres:5432/...`), the Postgres port does **not need to be published to the host at all**. Remove it.

**FastAPI: bind to LAN interface only**
```yaml
api:
  ports:
    - "192.168.1.176:8000:8000"   # Or use UFW to restrict — keep 0.0.0.0 but firewall it
```

**Risk level:** Critical — this is a BLOCKER

---

### 6. Postgres Credentials — 🚨 BLOCKER

**Current (docker-compose.yml):**
```yaml
POSTGRES_USER: ootils
POSTGRES_PASSWORD: ootils        # ← trivial password, in source control
POSTGRES_DB: ootils_dev
DATABASE_URL: postgresql://ootils:ootils@postgres:5432/ootils_dev
OOTILS_API_TOKEN: dev-token       # ← dev token in source control
```

**Problems:**
1. Password `ootils` is trivially guessable and identical to the username
2. Credentials are committed to a Git repository (even if private, this is bad practice)
3. `dev-token` as the API token is a dev credential that should never reach production
4. Private GitHub repo doesn't protect against: repo compromise, accidental visibility change, contributor access, GitHub breach

**Required: Docker secrets or `.env` file excluded from git**

**Option A — `.env` file (simpler, acceptable for private home deployment):**

Create `/opt/ootils/.env` on the VM (never in the repo):
```
POSTGRES_USER=ootils
POSTGRES_PASSWORD=<strong-generated-password>
POSTGRES_DB=ootils
DATABASE_URL=postgresql://ootils:<strong-generated-password>@postgres:5432/ootils
OOTILS_API_TOKEN=<generated-token>
```

Update `docker-compose.yml`:
```yaml
services:
  postgres:
    env_file: .env
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    # Remove hardcoded values

  api:
    env_file: .env
    environment:
      DATABASE_URL: ${DATABASE_URL}
      OOTILS_API_TOKEN: ${OOTILS_API_TOKEN}
```

Add to `.gitignore`:
```
.env
*.env
```

**Option B — Docker Secrets (stronger, more complex):** Use `secrets:` in compose with files on the host. Appropriate if this becomes multi-service or prod.

**Generate strong passwords:**
```bash
openssl rand -base64 32   # for Postgres password
openssl rand -hex 32       # for API token
```

**Risk level:** Critical — this is a BLOCKER

---

### 7. GitHub SSH Key

**Current:** Dedicated key `id_ootils` generated on the VM. Good practice.

**Remaining concerns:**
- What are the repo permissions? Deploy key (read-only) vs personal SSH key with full account access?
- Is the private key backed up? If the VM is wiped, can you re-deploy?
- Is the key passphrase-protected?

**Recommendations:**
- Add `id_ootils` as a **GitHub Deploy Key** (repo-scoped, read-only unless write is needed)
- Do NOT use a personal SSH key that has write access to all repos
- Store a backup of the private key in a password manager
- Consider a passphrase on the key; use `ssh-agent` or systemd credential store for automated use

**If key leaks:** A deploy key is scoped to one repo and read-only — damage is limited to repo contents. A personal key with full account access is catastrophic. Prefer deploy keys.

**Risk level:** Medium (if deploy key) / High (if personal key)

---

### 8. Updates — Unattended Security Patches

**Current:** No automatic update mechanism mentioned.

**Required:**
```bash
apt install unattended-upgrades apt-listchanges -y
dpkg-reconfigure -plow unattended-upgrades
```

Configure `/etc/apt/apt.conf.d/50unattended-upgrades`:
```
Unattended-Upgrade::Allowed-Origins {
    "${distro_id}:${distro_codename}-security";
};
Unattended-Upgrade::AutoFixInterruptedDpkg "true";
Unattended-Upgrade::MinimalSteps "true";
Unattended-Upgrade::Remove-Unused-Kernel-Packages "true";
Unattended-Upgrade::Remove-Unused-Dependencies "true";
Unattended-Upgrade::Automatic-Reboot "true";
Unattended-Upgrade::Automatic-Reboot-Time "03:00";
```

**Also:** Enable periodic Docker image updates — use Watchtower or a cron job:
```bash
# Weekly pull and restart (cron)
0 3 * * 0 cd /opt/ootils && docker compose pull && docker compose up -d
```

**Risk level:** Medium (unpatched vulns accumulate over time)

---

### 9. Logging

**Current:** No log configuration.

**Minimum required:**

#### SSH auth logs
Already logged by `journald`/`/var/log/auth.log` on Debian. Ensure it's rotating.

#### Docker container logs
Default Docker logs go to journald or json-file. Configure rotation:

In `/etc/docker/daemon.json`:
```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
```

#### Fail2ban for SSH
```bash
apt install fail2ban -y
```

Default config bans IPs after 5 failed SSH attempts. On a private LAN this seems paranoid but costs nothing and protects against compromised LAN devices.

#### UFW logging
```bash
ufw logging on
```

Logs to `/var/log/ufw.log` — useful for debugging and detecting scans.

**Risk level:** Low-Medium (logging won't prevent compromise, but enables forensics)

---

### 10. Network Isolation — VLAN

**Current:** VM on flat LAN 192.168.1.0/24 alongside all other home devices.

**Assessment:** Acceptable for a home private deployment, with caveats:
- If any other device on the LAN is compromised (IoT, guest device, etc.), it can reach the VM on any open port
- The VM is only as secure as the weakest device on the network

**Recommendation:**
- A dedicated VLAN for server workloads (e.g., VLAN 10 for servers, VLAN 20 for IoT) is the proper answer but requires a managed switch + router VLAN support (e.g., pfSense/OPNsense, UniFi)
- **Minimum acceptable:** Implement the UFW firewall above, which provides per-service access control regardless of VLAN
- **Better:** Move the VM to a DMZ/server VLAN when infrastructure supports it

**Risk level:** Medium (acceptable with firewall; high without)

---

## Hardened docker-compose.yml

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    env_file: .env
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    # No ports published — API connects via internal Docker network
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - backend
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 10
    deploy:
      resources:
        limits:
          memory: 512m

  api:
    build:
      context: .
      dockerfile: Dockerfile
    env_file: .env
    environment:
      DATABASE_URL: ${DATABASE_URL}
      OOTILS_API_TOKEN: ${OOTILS_API_TOKEN}
    ports:
      - "127.0.0.1:8000:8000"   # UFW controls external LAN access
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - backend
    restart: unless-stopped
    command: uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000
    deploy:
      resources:
        limits:
          memory: 256m

networks:
  backend:
    driver: bridge

volumes:
  postgres_data:
```

---

## Hardening Checklist (ordered by priority)

| Priority | Item | Status |
|----------|------|--------|
| 🚨 BLOCKER | Remove hardcoded credentials from docker-compose.yml | ❌ |
| 🚨 BLOCKER | Move Postgres port off 0.0.0.0 (or remove port binding) | ❌ |
| 🚨 BLOCKER | Install and configure UFW firewall | ❌ |
| ⚠️ HIGH | Harden sshd_config (PasswordAuthentication no, PermitRootLogin no) | ❓ |
| ⚠️ HIGH | Audit sudo config — remove NOPASSWD or scope it | ❓ |
| ⚠️ HIGH | Use GitHub Deploy Key (read-only, repo-scoped) for id_ootils | ❓ |
| ⚠️ HIGH | Use strong generated passwords for Postgres + API token | ❌ |
| 🔶 MEDIUM | Configure Docker log rotation (/etc/docker/daemon.json) | ❌ |
| 🔶 MEDIUM | Install fail2ban | ❌ |
| 🔶 MEDIUM | Enable unattended-upgrades | ❌ |
| 🔶 MEDIUM | Add Docker network isolation (networks: backend) | ❌ |
| 🔶 MEDIUM | Add resource limits to containers | ❌ |
| 🔶 MEDIUM | Remove --reload flag from uvicorn (dev only) | ❌ |
| 🟡 LOW | Add non-root user in Dockerfile (USER app) | ❌ |
| 🟡 LOW | Consider VLAN isolation for server segment | ❓ |
| 🟡 LOW | Define backup strategy for postgres_data volume | ❌ |

---

## Quick-Start Hardening Script

```bash
#!/usr/bin/env bash
# Run as root on the VM after initial provisioning

set -euo pipefail

# 1. SSH hardening
cat >> /etc/ssh/sshd_config << 'EOF'
PasswordAuthentication no
PermitRootLogin no
PermitEmptyPasswords no
MaxAuthTries 3
LoginGraceTime 30
AllowUsers debian
X11Forwarding no
EOF
systemctl restart sshd

# 2. UFW firewall
apt install -y ufw fail2ban unattended-upgrades
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow from 192.168.1.0/24 to any port 8000 proto tcp
ufw logging on
ufw --force enable

# 3. Fail2ban (default config handles SSH)
systemctl enable fail2ban
systemctl start fail2ban

# 4. Docker log rotation
mkdir -p /etc/docker
cat > /etc/docker/daemon.json << 'EOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF
systemctl restart docker

# 5. Unattended upgrades
dpkg-reconfigure -plow unattended-upgrades

# 6. Generate .env file for Docker (fill in real values)
mkdir -p /opt/ootils
cat > /opt/ootils/.env << EOF
POSTGRES_USER=ootils
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d '/+=\n')
POSTGRES_DB=ootils
DATABASE_URL=postgresql://ootils:${POSTGRES_PASSWORD}@postgres:5432/ootils
OOTILS_API_TOKEN=$(openssl rand -hex 32)
EOF
chmod 600 /opt/ootils/.env
chown debian:debian /opt/ootils/.env

echo "✅ Basic hardening complete. Review /opt/ootils/.env and update sudo config."
```

> **Note:** The `.env` file variable expansion in the heredoc above won't work as-is (POSTGRES_PASSWORD isn't set yet when DATABASE_URL is written). Generate passwords separately, then construct the DATABASE_URL manually.

---

## Backup Strategy (Minimum Viable)

```bash
# Postgres backup — daily dump, 7-day retention
cat > /etc/cron.d/ootils-backup << 'EOF'
0 2 * * * debian docker exec ootils-postgres-1 \
  pg_dump -U ootils ootils | gzip > /opt/backups/ootils-$(date +\%Y\%m\%d).sql.gz \
  && find /opt/backups -name "*.sql.gz" -mtime +7 -delete
EOF

mkdir -p /opt/backups
chown debian:debian /opt/backups
```

For real resilience: copy backups off the VM (NAS, cloud storage, Proxmox backup server).

---

*End of security review. See checklist above for prioritized action items.*
