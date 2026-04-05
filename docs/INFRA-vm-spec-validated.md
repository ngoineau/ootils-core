# INFRA — VM Creation Spec (Validated)

**Target:** Proxmox VE 9.1.6, kernel 6.17.13-2-pve  
**VM:** 201 `ootils-v1`, Debian 12 cloud image, 6 GiB RAM, 2 vCPU, 32 GiB disk  
**Reviewed:** 2026-04-05  
**Status:** ✅ Valid with corrections (see issues below)

---

## Issues Found

### 🔴 Critical

| # | Issue | Fix |
|---|-------|-----|
| C1 | **Missing SSH config for `id_ootils`** — `git clone git@github.com:...` uses the default key (`~/.ssh/id_ed25519`), not `~/.ssh/id_ootils`. Clone will fail silently with auth error. | Add `~/.ssh/config` entry before cloning (see Step 6). |
| C2 | **No `.env` / environment variables addressed** — `docker compose up` will fail or use defaults/empty values if the app requires secrets (DB URL, API keys, etc.). No mention of this in the spec. | Add explicit step to create/copy `.env` before `docker compose up`. |

### 🟡 Important

| # | Issue | Fix |
|---|-------|-----|
| I1 | **`sleep 30` too short for cloud-init first boot** — First boot runs cloud-init, provisions SSH keys, configures network, optionally runs user-data scripts. On a 2-vCPU VM, 30s is marginal. Often takes 60–120s. If you SSH too early you'll get a connection refused and assume something broke. | Use `sleep 90` or poll with `qm agent 201 ping` until responsive. |
| I2 | **`sleep 15` too short after `docker compose up --build -d`** — You're building images + starting containers. 15s is almost certainly not enough. DB migrations run after this with `exec`, which will fail if the container isn't ready. | Use `docker compose ps` / `docker compose logs` to verify health, or add a `depends_on` healthcheck in compose. Minimum `sleep 30` if static. |
| I3 | **`newgrp docker` in a session flow** — In an interactive shell, `newgrp docker` opens a subshell. Subsequent commands in the _same script_ won't inherit the group. In practice (manual execution), you need to either log out/back in or run subsequent docker commands with `sudo`. | Log out and reconnect after `usermod`, then continue. |

### 🟢 Minor / Good to Know

| # | Note |
|---|------|
| M1 | `--cpu host` prevents live migration — acceptable for single-node homelab, intentional tradeoff. If you ever add a node, you'll need to change this. |
| M2 | `--sshkeys /root/.ssh/authorized_keys` will inject ALL keys in that file into the VM. Likely intentional, but be aware. Consider using a dedicated pubkey file. |
| M3 | `qm resize 201 scsi0 32G` is **absolute**, not additive (`+32G`). The cloud image is ~2 GB so the result is a 32 GB disk — which is correct. Just be explicit in your mental model. |
| M4 | No `--searchdomain` in cloud-init. Not required, but useful for local DNS resolution (e.g. `--searchdomain home.local`). |
| M5 | `--ide2 local:cloudinit` — `local` is dir-type storage, which correctly supports cloud-init images. ✅ |
| M6 | `qm importdisk` on `local-lvm` (lvmthin) — Proxmox will convert the qcow2 to a raw LV and name it `vm-201-disk-0`. The subsequent `--scsi0 local-lvm:vm-201-disk-0` reference is correct. ✅ |
| M7 | PVE 9 defaults to `q35` machine type — fine for this workload, no need to override. |
| M8 | `virtio-scsi-pci` controller is correct. `virtio-scsi-single` is available in newer PVE and more performant for single-disk setups, but `virtio-scsi-pci` is solid and compatible. |

---

## Validated & Corrected Spec

```bash
# ============================================================
# INFRA: ootils-v1 VM on Proxmox VE 9.1.6
# VM ID: 201 | IP: 192.168.1.176 | Storage: local-lvm
# ============================================================

# ── Step 1: Download Debian 12 cloud image ──────────────────
ssh root@192.168.1.175
cd /var/lib/vz/template/iso
wget -O debian-12-genericcloud-amd64.qcow2 \
  https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-genericcloud-amd64.qcow2

# ── Step 2: Create VM ────────────────────────────────────────
qm create 201 \
  --name ootils-v1 \
  --memory 6144 \
  --cores 2 \
  --cpu host \
  --net0 virtio,bridge=vmbr0 \
  --serial0 socket \
  --vga serial0 \
  --ostype l26 \
  --agent enabled=1 \
  --onboot 1

qm importdisk 201 /var/lib/vz/template/iso/debian-12-genericcloud-amd64.qcow2 local-lvm

qm set 201 \
  --scsihw virtio-scsi-pci \
  --scsi0 local-lvm:vm-201-disk-0 \
  --boot order=scsi0 \
  --ide2 local:cloudinit

# Resize to 32G absolute (cloud image base is ~2G)
qm resize 201 scsi0 32G

# ── Step 3: Cloud-Init ───────────────────────────────────────
# NOTE: authorized_keys injects ALL keys in that file.
# Use a dedicated pubkey file if you want to be selective.
qm set 201 \
  --ciuser debian \
  --sshkeys /root/.ssh/authorized_keys \
  --ipconfig0 ip=192.168.1.176/24,gw=192.168.1.1 \
  --nameserver 1.1.1.1

# ── Step 4: Start & wait for cloud-init ─────────────────────
qm start 201

# Wait for guest agent (more reliable than a fixed sleep)
echo "Waiting for VM to come up..."
for i in $(seq 1 30); do
  qm agent 201 ping 2>/dev/null && echo "VM ready!" && break
  echo "  ...attempt $i/30, sleeping 5s"
  sleep 5
done

# Confirm reachability
ping -c 3 192.168.1.176
ssh debian@192.168.1.176

# ── Step 5: First boot — guest agent + Docker ────────────────
# Run inside the VM as debian user

sudo apt-get update -qq
sudo apt-get install -y qemu-guest-agent
sudo systemctl enable --now qemu-guest-agent

sudo apt-get install -y ca-certificates curl gnupg git

# Docker repo + GPG
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/debian bookworm stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y \
  docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin

sudo usermod -aG docker debian

# !! Log out and reconnect for group membership to take effect !!
# newgrp docker does NOT persist across the session boundary in a script.
exit
ssh debian@192.168.1.176  # reconnect — docker group now active
docker info               # verify

# ── Step 6: GitHub SSH key + deploy ─────────────────────────
ssh-keygen -t ed25519 -C 'ootils-v1' -f ~/.ssh/id_ootils -N ''
cat ~/.ssh/id_ootils.pub
# → Add this key to GitHub Settings > SSH keys

# REQUIRED: Configure SSH to use this key for GitHub
cat >> ~/.ssh/config << 'EOF'
Host github.com
  IdentityFile ~/.ssh/id_ootils
  StrictHostKeyChecking accept-new
EOF
chmod 600 ~/.ssh/config

# Test GitHub auth before cloning
ssh -T git@github.com

git clone git@github.com:ngoineau/ootils-core.git
cd ootils-core
git checkout live/v1-bootstrap

# REQUIRED: Set up environment variables before starting
# Copy or create .env — this is NOT optional if the app needs secrets
# cp /path/to/your/.env.production .env
# OR create it manually:
# cat > .env << 'EOF'
# DATABASE_URL=...
# SECRET_KEY=...
# EOF

docker compose up --build -d

# Wait for services to be healthy (don't rely on a fixed sleep)
echo "Waiting for api container..."
for i in $(seq 1 20); do
  docker compose ps api | grep -q "healthy\|running" && break
  echo "  ...attempt $i/20, sleeping 5s"
  sleep 5
done
docker compose ps  # review status

# Run migrations and seed
docker compose exec api python -m ootils_core.db.migrate
docker compose exec api python scripts/seed_demo_data.py

# ── Step 7: Validate ─────────────────────────────────────────
curl -sf http://192.168.1.176:8000/health && echo "✅ Health OK"
curl -sf http://192.168.1.176:8000/v1/issues | head -c 500
```

---

## Checklist Before You Run

- [ ] `192.168.1.176` is not already in use on the network
- [ ] `/root/.ssh/authorized_keys` on the Proxmox host contains the key(s) you want in the VM
- [ ] `.env` file exists or you know what env vars the app needs
- [ ] `live/v1-bootstrap` branch exists on `ngoineau/ootils-core`
- [ ] Docker Hub / image registry accessible from the VM (if any images are pulled, not just built)
- [ ] GitHub SSH key added before `git clone`

---

## Resource Sanity Check

| Resource | VM 201 | VM 200 | Plex CT | Host Total |
|----------|--------|--------|---------|------------|
| RAM (GiB) | 6 | 12 | ~2 est. | 62 |
| vCPU | 2 | — | — | 4 |
| Disk | 32 GiB (lvm) | 100 GiB (lvm) | — | 770+ GB lvm free |

Allocation looks reasonable. 6 GiB for a Docker host running an API + DB is workable. Monitor actual usage post-deploy.
