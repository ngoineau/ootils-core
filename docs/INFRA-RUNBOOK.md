# Documentation complete infrastructure OOTILS

Mise a jour: 2026-04-10

## 1. Objectif

Ce document sert de reference de reconstruction de l'infrastructure OOTILS telle qu'elle a ete verifiee en production au 2026-04-05.

Il couvre:
- les machines et IP
- les modes d'acces verifies
- les chemins applicatifs
- les services Docker actifs
- les ports publies
- les scripts d'exploitation cote poste admin Windows
- les procedures de redeploiement et de validation
- le registre des secrets a restaurer hors GitHub

Important:
- ce document est volontairement publie sans mots de passe ni tokens en clair
- les secrets doivent etre restaures depuis le coffre local, le keyring, les fichiers d'environnement sur les hotes, ou les magasins d'auth applicatifs
- si un incident majeur impose une reconstruction, il faut considerer les anciens secrets comme potentiellement compromis et les faire tourner

## 2. Vue d'ensemble

L'infrastructure OOTILS actuellement exploitee repose sur 4 couches:

1. Proxmox sur 192.168.1.175
2. VM Docker 200 sur 192.168.1.124, qui heberge OpenClaw et ootils-ui
3. Serveur backend ootils-v1 sur 192.168.1.176, qui heberge ootils-core
4. Poste admin Windows C:\dev\OpenClaw, qui maintient les tunnels SSH locaux et les scripts d'exploitation

Topologie fonctionnelle:

```text
Poste admin Windows
  C:\dev\OpenClaw
    |
    | SSH tunnel local
    | 127.0.0.1:18789 -> 192.168.1.124:127.0.0.1:18789
    | 127.0.0.1:18790 -> 192.168.1.124:127.0.0.1:18790
    | 127.0.0.1:13000 -> 192.168.1.124:127.0.0.1:13000
    v
VM Docker 200 - 192.168.1.124
  /opt/openclaw   -> OpenClaw gateway
  /opt/ootils-ui  -> Next.js UI Dockerisee

Acces direct LAN separe
  192.168.1.176:8000 -> ootils-core API
```

## 3. Inventaire des hotes et acces verifies

### 3.1 Proxmox

- Role: hyperviseur principal
- Hostname: proxmox
- IP: 192.168.1.175
- Web: https://192.168.1.175:8006
- SSH: ssh root@192.168.1.175
- Bridge LAN: vmbr0
- Passerelle LAN: 192.168.1.1

Etat connu:
- Proxmox VE 9.1.6
- kernel 6.17.13-2-pve
- acces SSH root verifie depuis le poste admin

### 3.2 VM Docker 200

- Role: hote Docker pour OpenClaw et ootils-ui
- VMID: 200
- Hostname observe: ubuntu
- IP: 192.168.1.124
- SSH: ssh root@192.168.1.124
- OS: Ubuntu 24.04 cloud image
- Docker: installe et actif
- Docker Compose: installe et actif

Etat connu:
- le projet Compose OpenClaw tourne depuis /opt/openclaw
- ootils-ui tourne depuis /opt/ootils-ui
- IP en DHCP selon la documentation historique, donc a reserver si on veut figer la reconstruction

### 3.3 Backend OOTILS

- Role: API ootils-core + PostgreSQL
- Hostname: ootils-v1
- IP: 192.168.1.176
- SSH: ssh debian@192.168.1.176
- Port API publie: 8000/tcp
- Acces direct LAN: http://192.168.1.176:8000

Etat connu au 2026-04-10:
- repo: git@github.com:ngoineau/ootils-core.git (SSH, pas HTTPS)
- branche: main
- commit deploye: 81c520f (fix migration 015 — dedup uom_conversions)
- remote git: correctement configure en SSH avec cle id_ed25519_github (voir section 4.1)
- conteneurs en service:
  - ootils-core-api-1
  - ootils-core-postgres-1
- migrations appliquees: 17/17 (001 → 017), trackees dans schema_migrations

### 3.4 Poste admin Windows

- Role: exploitation et acces admin
- Workspace: C:\dev\OpenClaw
- Tunnel local gere par tache planifiee Windows
- Commande surveillee: powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\dev\OpenClaw\scripts\Ensure-OpenClawTunnel.ps1"

## 4. Acces admin et identites

### 4.1 Identites SSH verifiees

Depuis le poste admin Windows:
- root@192.168.1.175 -> ~/.ssh/id_ed25519_ootils
- root@192.168.1.124 -> ~/.ssh/id_ed25519_ootils
- debian@192.168.1.176 -> ~/.ssh/id_ed25519_ootils avec IdentitiesOnly=yes

Depuis Claw (OpenClaw sur VM 200, ajout 2026-04-08):
- Cle infra: ~/.ssh/id_ed25519_infra (ed25519, label: claw@openclaw)
  - debian@192.168.1.176 (alias: ootils-core-vm201)
  - root@192.168.1.124   (alias: openclaw-vm200)
- Cle GitHub: ~/.ssh/id_ed25519_github
  - utilisee pour git sur ootils-core (VM 201) et ootils-ui (VM 200)
  - copiee sur les deux VMs dans ~/.ssh/id_ed25519_github

Configuration ~/.ssh/config cote Claw:
```
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_github
  IdentitiesOnly yes

Host ootils-core-vm201
  HostName 192.168.1.176
  User debian
  IdentityFile ~/.ssh/id_ed25519_infra
  IdentitiesOnly yes

Host openclaw-vm200
  HostName 192.168.1.124
  User root
  IdentityFile ~/.ssh/id_ed25519_infra
  IdentitiesOnly yes
```

En cas de reconstruction: regenerer id_ed25519_infra, reposter la cle publique sur les VMs dans ~/.ssh/authorized_keys.

### 4.2 GitHub CLI

- gh CLI connecte sur le poste admin
- compte actif: ngoineau
- scopes verifies: gist, read:org, repo

Usage:
- consultation et commentaire d'issues GitHub
- publication de documentation operationnelle

### 4.3 Registre des secrets a restaurer hors GitHub

Les secrets ne doivent pas etre republies dans GitHub. Pour reconstruire, verifier ces emplacements:

1. Poste admin Windows
- cle SSH privee: %USERPROFILE%\.ssh\id_ed25519_ootils
- eventuelles entrees ssh-agent associees
- authentification gh stockee dans le keyring Windows

2. VM 200 OpenClaw
- secrets OpenClaw dans /root/.openclaw/auth-profiles.json
- magasin actif OpenClaw dans /root/.openclaw/agents/main/agent/auth-profiles.json
- variables d'environnement de stack attendues dans /opt/openclaw/.env

3. Backend ootils-core
- variables d'environnement attendues dans ~/ootils-core/.env
- elements a verifier au minimum:
  - POSTGRES_USER
  - POSTGRES_PASSWORD
  - POSTGRES_DB
  - OOTILS_API_TOKEN

4. A faire en cas de sinistre
- restaurer les secrets depuis le coffre local ou la sauvegarde securisee
- faire tourner les tokens/API keys si l'hote a ete perdu ou compromis
- verifier que les vieux fichiers de backup ne contiennent pas encore des secrets historiques

## 5. OOTILS UI

### 5.1 Emplacement et etat courant

- Hote: 192.168.1.124
- Chemin repo: /home/ubuntu/ootils-ui
- Remote git: git@github.com:ngoineau/ootils-ui.git (branche master)
- Commit deploye au 2026-04-10: da604c8 (GraphViz, ScenarioContext, hooks nouveaux)
- Container: ootils-ui
- Port: 3000

### 5.2 Publication reseau

Compose actuel (2026-04-08):

```yaml
services:
  ootils-ui:
    build: .
    container_name: ootils-ui
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      - NODE_ENV=production
      - OOTILS_API_URL=http://192.168.1.176:8000
```

Acces:
- Directement sur le LAN: http://192.168.1.124:3000
- Proxy API integre Next.js: /api/* -> http://192.168.1.176:8000/*

Validation:
- curl http://192.168.1.124:3000/ -> HTTP 200 + HTML Next.js

### 5.3 Redeploiement UI (Claw autonome)

Claw peut deployer directement via SSH:

```bash
ssh openclaw-vm200 "cd /home/ubuntu/ootils-ui && git pull && docker compose up -d --build"
```

Reconstruction from scratch sur VM 200:
```bash
ssh openclaw-vm200 "
  # Prerequis: cle GitHub sur VM (voir section 4.1)
  git clone git@github.com:ngoineau/ootils-ui.git /home/ubuntu/ootils-ui
  cd /home/ubuntu/ootils-ui
  docker compose up -d --build
"
```

Commande type:

```bash
ssh root@192.168.1.124 'bash -s' < C:/dev/OpenClaw/scripts/deploy-ootils-ui-vm.sh
```

Ou directement sur la VM:

```bash
cd /opt/ootils-ui
git fetch --all --tags
git checkout <commit>
docker compose up -d --build
curl -I http://127.0.0.1:13000
```

## 6. OOTILS backend

### 6.1 Emplacement et etat courant

- Hote: 192.168.1.176
- Utilisateur d'exploitation verifie: debian
- Chemin repo: ~/ootils-core
- Remote git: git@github.com:ngoineau/ootils-core.git (SSH)
- Branche: main
- Commit deploye au 2026-04-10: 81c520f

### 6.2 Topologie Docker Compose

Compose verifie:

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 10

  api:
    build:
      context: .
      dockerfile: Dockerfile
    environment:
      DATABASE_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
      OOTILS_API_TOKEN: ${OOTILS_API_TOKEN}
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy
    command: uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000

volumes:
  postgres_data:
```

### 6.3 Etat de service verifie

Le 2026-04-05:
- ootils-core-api-1 -> Up
- ootils-core-postgres-1 -> Up (healthy)
- port expose: 0.0.0.0:8000->8000/tcp

### 6.4 Redeploiement backend

Procedure standard:

```bash
ssh -i ~/.ssh/id_ed25519_ootils -o IdentitiesOnly=yes debian@192.168.1.176
cd ~/ootils-core
git pull origin main
docker compose up -d --build api
docker compose ps
```

### 6.5 Migrations et seed

Systeme de migration au 2026-04-10:
- Les migrations sont appliquees automatiquement par OotilsDB._apply_migrations() au demarrage de l'API
- Systeme incrémental avec table de tracking schema_migrations (PR #136)
- Advisory lock PostgreSQL : une seule instance applique les migrations a la fois
- 17 migrations actives : 001_initial_schema → 017_shortage_severity_class
- Au demarrage : seules les migrations absentes de schema_migrations sont appliquees (O(1) si tout est a jour)

Verification de l'etat des migrations:
```bash
docker exec ootils-core-postgres-1 psql -U ootils -d ootils_dev \
  -c "SELECT version, applied_at FROM schema_migrations ORDER BY applied_at;"
```

Application manuelle d'une migration si besoin (cas exceptionnel):
```bash
docker exec ootils-core-postgres-1 psql -U ootils -d ootils_dev \
  < src/ootils_core/db/migrations/XXX_nom.sql
# Puis enregistrer dans le tracker :
docker exec ootils-core-postgres-1 psql -U ootils -d ootils_dev \
  -c "INSERT INTO schema_migrations (version) VALUES ('XXX_nom.sql') ON CONFLICT DO NOTHING;"
```

Relance du seed enrichi:
```bash
ssh ootils-core-vm201 "cd ~/ootils-core && docker compose exec -T api python scripts/seed_demo_data.py"
```

### 6.6 Validation fonctionnelle backend

Validations de reference utilisees:

```bash
curl -H "Authorization: Bearer <OOTILS_API_TOKEN>" "http://192.168.1.176:8000/v1/issues?horizon_days=90"
curl -H "Authorization: Bearer <OOTILS_API_TOKEN>" "http://192.168.1.176:8000/v1/graph?root_item_code=VALVE-02&root_location=DC-LAX&horizon_days=90"
```

Validation finale observee le 2026-04-05:
- graph: NODES=105, EDGES=15
- edge types: consumes=13, replenishes=2
- issues: total=13

## 7. OpenClaw sur la VM 200

### 7.1 Emplacement et etat courant

- Hote: 192.168.1.124
- Chemin: /opt/openclaw
- Compose project: openclaw
- Container principal: openclaw-openclaw-gateway-1
- Etat constate: Up 7 days (healthy) au moment du releve

### 7.2 Compose verifie

Le compose utilise:
- un service openclaw-gateway
- un service openclaw-cli en network_mode service:openclaw-gateway
- des ports lies a des variables d'environnement

Ports attendus apres durcissement:
- OPENCLAW_GATEWAY_PORT=127.0.0.1:18789
- OPENCLAW_BRIDGE_PORT=127.0.0.1:18790

Consequences:
- OpenClaw ne doit plus etre expose directement sur le LAN
- l'acces se fait via tunnel SSH depuis le poste admin

Volumes critiques:
- ${OPENCLAW_CONFIG_DIR} -> /home/node/.openclaw
- ${OPENCLAW_WORKSPACE_DIR} -> /home/node/.openclaw/workspace

Donnees critiques a sauvegarder:
- /root/.openclaw
- /root/.openclaw/workspace
- /opt/openclaw/.env

### 7.3 Validation OpenClaw

Tests de reference:

```bash
cd /opt/openclaw
docker compose ps
curl http://127.0.0.1:18789/healthz
docker logs --since 12h openclaw-openclaw-gateway-1
docker exec openclaw-openclaw-gateway-1 node dist/index.js security audit
```

## 8. Tunnel Windows de reprise admin

### 8.1 Script local

Fichier:
- C:\dev\OpenClaw\scripts\Ensure-OpenClawTunnel.ps1

Comportement actuel:
- script one-shot idempotent
- mutex global pour eviter les doublons
- journal local dans C:\dev\OpenClaw\logs\openclaw-tunnel.log
- verifie les ports 18789 et 18790 localement
- si tunnel casse, relance ssh.exe avec les forwards suivants:

```text
-L 18789:127.0.0.1:18789
-L 18790:127.0.0.1:18790
-L 13000:127.0.0.1:13000
root@192.168.1.124
```

### 8.2 Tache planifiee Windows

Nom:
- OpenClaw SSH Tunnel

Etat releve:
- State: Ready
- Triggers:
  - Logon trigger
  - Time trigger avec repetition PT15M
- Action:
  - powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\dev\OpenClaw\scripts\Ensure-OpenClawTunnel.ps1"

Parametres durcis historiquement:
- ExecutionTimeLimit=PT0S
- redemarrage sur echec configure
- une seule instance a la fois via mutex dans le script

### 8.3 Verification locale du tunnel

Depuis le poste admin:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18789/healthz
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:13000
Get-Content C:\dev\OpenClaw\logs\openclaw-tunnel.log -Tail 100
```

## 9. GitHub et livraisons connues

### 9.1 Repositories

- ngoineau/ootils-core
- ngoineau/ootils-ui
- openclaw/openclaw

### 9.2 Livraisons confirmees

- ootils-core#46 -> UI initiale deployee sur VM 200
- ootils-core#67 -> merge ingest-router + ghosts-tags sur main
- ootils-core#83 -> SSH Claw setup (cle id_ed25519_infra)
- ootils-core#84 -> fix VM 201 git remote + deploiement
- ootils-ui#3   -> deploiement VM 200 Dockerfile+compose
- ootils-core#136 -> migration runner schema_migrations + advisory lock + fixes securite
- ootils-core#149 -> 30 fichiers tests + suppression modules legacy
- ootils-core#161 -> 4 findings QC critiques/high (SQL injection, hash lock, auth startup, migration silent exception)
- ootils-core#162 -> allocation SELECT FOR UPDATE
- ootils-core#163 -> DQ ANY() chunking
- ootils-core#164 -> 3 findings medium (orphaned edges, bucket boundary, health check timeout)
- ootils-core#165 -> fix .dockerignore scripts/
- ootils-core#166 -> fix migration 014 submitted_at
- ootils-core#167 -> fix migration runner row_factory dict_row
- ootils-core#168 -> fix migration 015 dedup uom_conversions

## 10. Procedure de reconstruction complete apres crash

### Etape 1 - Recuperer les acces admin

1. Restaurer le poste Windows admin ou un poste equivalent
2. Restaurer la cle privee %USERPROFILE%\.ssh\id_ed25519_ootils
3. Verifier gh auth status
4. Verifier SSH vers:
   - root@192.168.1.175
   - root@192.168.1.124
   - debian@192.168.1.176

### Etape 2 - Restaurer Proxmox

1. Verifier que 192.168.1.175 repond
2. Ouvrir https://192.168.1.175:8006
3. Verifier que la VM 200 est demarree
4. Si reconstruction hyperviseur necessaire:
   - recreer bridge vmbr0 sur le LAN
   - recreer la VM 200 Ubuntu 24.04
   - reconfigurer Docker et Docker Compose

### Etape 3 - Restaurer la VM 200

1. SSH root@192.168.1.124
2. Reinstaller Docker et Docker Compose si necessaire
3. Restaurer /root/.openclaw depuis sauvegarde
4. Restaurer /opt/openclaw/.env
5. Relancer OpenClaw:

```bash
cd /opt/openclaw
docker compose up -d
docker compose ps
curl http://127.0.0.1:18789/healthz
```

6. Copier la cle GitHub sur la VM (pour le clone ootils-ui):

```bash
# Depuis Claw
scp ~/.ssh/id_ed25519_github root@192.168.1.124:~/.ssh/id_ed25519_github
ssh root@192.168.1.124 "chmod 600 ~/.ssh/id_ed25519_github && cat > ~/.ssh/config << 'EOF'
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_github
  IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config"
```

7. Cloner et deployer ootils-ui:

```bash
ssh openclaw-vm200 "
  git clone git@github.com:ngoineau/ootils-ui.git /home/ubuntu/ootils-ui
  cd /home/ubuntu/ootils-ui
  docker compose up -d --build
"
# Valider
curl http://192.168.1.124:3000/
```

### Etape 4 - Restaurer le backend ootils-v1

1. SSH debian@192.168.1.176
2. Reinstaller Docker et Docker Compose si necessaire
3. Copier la cle GitHub sur la VM:

```bash
# Depuis Claw
scp ~/.ssh/id_ed25519_github debian@192.168.1.176:~/.ssh/id_ed25519_github
ssh ootils-core-vm201 "chmod 600 ~/.ssh/id_ed25519_github && cat > ~/.ssh/config << 'EOF'
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_github
  IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config"
```

4. Cloner le repo (SSH, pas HTTPS):

```bash
ssh ootils-core-vm201 "
  git clone git@github.com:ngoineau/ootils-core.git ~/ootils-core
  cd ~/ootils-core
  git checkout main
"
```

5. Restaurer ~/ootils-core/.env
6. Lancer PostgreSQL et l'API:

```bash
ssh ootils-core-vm201 "cd ~/ootils-core && docker compose up -d --build"
```

7. Relancer le seed si base vide:

```bash
ssh ootils-core-vm201 "cd ~/ootils-core && docker compose exec -T api python scripts/seed_demo_data.py"
```

8. Valider:

```bash
curl -H "Authorization: Bearer dev-token" "http://192.168.1.176:8000/v1/issues?horizon_days=90"
curl http://192.168.1.176:8000/health
```

### Etape 5 - Restaurer les tunnels admin Windows

1. Verifier la presence de C:\dev\OpenClaw\scripts\Ensure-OpenClawTunnel.ps1
2. Verifier la tache planifiee OpenClaw SSH Tunnel
3. Executer un run manuel si besoin
4. Valider les URLs locales:

```text
http://127.0.0.1:18789
http://127.0.0.1:13000
```

## 11. Sauvegardes minimales a ne jamais perdre

### Priorite haute

- %USERPROFILE%\.ssh\id_ed25519_ootils (poste admin Windows)
- ~/.ssh/id_ed25519_github (Claw — acces GitHub depuis VMs)
- ~/.ssh/id_ed25519_infra (Claw — acces SSH aux VMs)
- contenu de /root/.openclaw
- /opt/openclaw/.env
- ~/ootils-core/.env (VM 201)
- dumps ou volume postgres_data du backend (VM 201)

### Priorite moyenne

- C:\dev\OpenClaw\scripts
- C:\dev\OpenClaw\logs
- /home/ubuntu/ootils-ui — pas critique (reclonable depuis GitHub a tout moment)
- documentation d'exploitation dans C:\dev\OpenClaw

### Backup Postgres quotidien (VM 201)

- Script repo: `scripts/backup_postgres.sh`
- Install cron: `scripts/install_backup_cron.sh`
- Repertoire par defaut: `~/ootils-backups/postgres`
- Retention par defaut: `7` jours
- Installation type:

```bash
cd ~/ootils-core
chmod +x scripts/backup_postgres.sh scripts/install_backup_cron.sh
BACKUP_CRON_SCHEDULE="15 2 * * *" scripts/install_backup_cron.sh
scripts/backup_postgres.sh
```

- Verification rapide:

```bash
ls -lah ~/ootils-backups/postgres
crontab -l
```

## 12. Risques et points faibles restants

1. VM 200 en DHCP: risque de changement d'IP si pas de reservation
2. Tunnel Windows: solution pratique mais pas ideale structurellement
3. Secrets distribues sur plusieurs emplacements: necessite un vrai coffre de secrets (issue ouverte)
4. Dump Postgres quotidien local OK si `scripts/install_backup_cron.sh` est installe sur VM 201; la copie hors machine reste a faire.
5. Snapshots Proxmox VM 200 + VM 201 non encore automatises (a faire)

## 13. Checklist rapide post-reconstruction

- SSH Proxmox OK
- SSH VM 200 OK
- SSH ootils-v1 OK
- OpenClaw healthz OK sur 127.0.0.1:18789
- ootils-ui HTTP 200 sur 192.168.1.124:3000
- ootils-core API UP sur 192.168.1.176:8000
- curl http://192.168.1.176:8000/health -> {"status":"ok","version":"1.0.0"}
- /v1/issues repond avec Bearer token
- /v1/scenarios repond avec Bearer token
- schema_migrations contient 17 lignes
- tache Windows OpenClaw SSH Tunnel presente
- secrets restaures et rotates si incident de securite

## 14. Conclusion operationnelle

Si le poste admin, la VM 200 ou ootils-v1 tombent, la reconstruction est possible avec:
- la cle SSH admin
- les secrets restaures hors GitHub
- les repos GitHub
- ce document

Le point le plus critique n'est pas le code: ce sont les secrets, la persistance OpenClaw, et la configuration du backend .env / base PostgreSQL.
