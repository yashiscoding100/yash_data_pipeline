# Free Cloud Service Deployment Guide

The competition rules state:
> *"Cloud side: any free cloud provider or free tier, for receiving, storing and processing the data... Using a paid service disqualifies the submission."*

The cloud service (`cloud/server.py`) has **zero external dependencies** and runs natively on any standard Python 3.10+ runtime. Below are 1-click / simple deployment guides for top free providers.

---

## Option 1: Render.com (Recommended - 100% Free Forever)

Render provides a free Web Service tier with public HTTPS URLs:

1. Push your repository to GitHub / GitLab.
2. Sign up at [Render.com](https://render.com) (free account).
3. Click **New +** -> **Web Service**.
4. Connect your repository.
5. Configure the service:
   - **Environment**: `Python 3`
   - **Build Command**: `echo "No build required"`
   - **Start Command**: `python -m cloud.server --port $PORT`
6. Click **Deploy Web Service**.
7. Copy your assigned HTTPS URL (e.g., `https://never-lose-a-heartbeat.onrender.com`).
8. On the Raspberry Pi, run the pipeline pointing to your Render cloud URL:
   ```bash
   ./pipeline run --cloud-url https://never-lose-a-heartbeat.onrender.com
   ```

---

## Option 2: Fly.io (Free Tier)

Fly.io offers free-tier VMs:

1. Install `flyctl`:
   ```bash
   curl -L https://fly.io/install.sh | sh
   ```
2. Initialize app in the project root:
   ```bash
   fly launch --no-deploy
   ```
3. Use the provided `Dockerfile` or specify the start command in `fly.toml`:
   ```toml
   [processes]
     app = "python -m cloud.server --port 8080"

   [[services]]
     internal_port = 8080
     protocol = "tcp"

     [[services.ports]]
       port = 80
       handlers = ["http"]

     [[services.ports]]
       port = 443
       handlers = ["tls", "http"]
   ```
4. Deploy:
   ```bash
   fly deploy
   ```
5. Your endpoint is `https://<app-name>.fly.dev`.

---

## Option 3: Free-Tier VPS (Oracle Cloud Always-Free or AWS EC2 t4g.small)

If using a free cloud VM:

1. SSH into the free VM:
   ```bash
   ssh ubuntu@<VM_PUBLIC_IP>
   ```
2. Clone repository:
   ```bash
   git clone <REPO_URL>
   cd never-lose-a-heartbeat
   ```
3. Run cloud server:
   ```bash
   python3 -m cloud.server --port 8000
   ```
4. On the Raspberry Pi, target `http://<VM_PUBLIC_IP>:8000`.

---

## Option 4: Local Evaluation / Judging Mode

When running evaluation locally on the same network or test harness:

1. Start Cloud Server:
   ```bash
   python3 pipeline.py cloud --port 8000
   ```
2. Start Mock Sensor Service:
   ```bash
   python3 pipeline.py mock --port 7100 --mode signal --seed 7
   ```
3. Start Device Pipeline:
   ```bash
   python3 pipeline.py run --mock-port 7100 --cloud-url http://127.0.0.1:8000
   ```
