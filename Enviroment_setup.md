# 🛠️ Synthetic Data Warehouse Dev Environment Setup

Welcome! This guide will walk you through setting up a fully containerized development environment with:

- 📦 MariaDB (simulated warehouse database)
- 💻 VSCode Server (browser-based coding environment)
- 🧠 Adminer (SQL GUI for database browsing)
- 🐍 Jupyter Notebook (for exploring data with Python)

This setup uses Docker, so there’s **no need to install Python, MariaDB, or Jupyter manually** — everything runs in isolated containers.

---

## ✅ Prerequisites

Make sure you have the following installed on your machine:

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- A modern web browser (Chrome, Firefox, Edge, etc.)

---

## 📁 Step 1: Create Your Project Folder

Open a terminal and run:

```bash
mkdir synthetic-data-warehouse
cd synthetic-data-warehouse
```

## 📄 Step 3: Create docker-compose.yml
Create a new file called docker-compose.yml and paste in the contents from the code block at the bottom titled docker-compose.yml.

```
version: '3.8'

services:

  mariadb:
    image: mariadb:11
    container_name: mariadb
    restart: always
    environment:
      MARIADB_ROOT_PASSWORD: rootpass
      MARIADB_DATABASE: warehouse
      MARIADB_USER: trainee
      MARIADB_PASSWORD: trainpass
    volumes:
      - mariadb_data:/var/lib/mysql
    ports:
      - "3306:3306"

  vscode:
    image: lscr.io/linuxserver/code-server:latest
    container_name: vscode
    environment:
      - PUID=1000
      - PGID=1000
      - PASSWORD=changeme
    volumes:
      - ./workspace:/config/workspace
    ports:
      - "8443:8443"
    restart: unless-stopped

  adminer:
    image: adminer
    container_name: adminer
    restart: always
    ports:
      - "8080:8080"

volumes:
  mariadb_data:
```

This sets up three services:

- mariadb (the database)
- vscode (browser-based code editor)
- adminer (web SQL interface)

## ▶️ Step 3: Start the Environment
From your terminal, run:
```
docker-compose up -d
```
Docker will:
- Download the necessary images
- Launch all services in the background

## 🔌 Step 4: Open Your Tools
### 💻 VSCode Server
Open in your browser:
👉 [https://localhost:8443](https://localhost:8443)
Login with password: changeme

You might need to bypass a browser security warning — it's safe locally.

### 🧠 Adminer (SQL Admin Tool)
Open in your browser:
👉 [http://localhost:8080](https://localhost:8080)

Login details:
|-----|----|
|System | MariaDB|
|Server | mariadb|
|Username | trainee| 
|Password | trainpass|
| Database|  warehouse|

## 🧩 Step 5: Install Extensions in VSCode
Once logged into VSCode in your browser:

1. Click the Extensions icon on the left.
2. Search for and install:
3. Python (by Microsoft)
4. Jupyter (by Microsoft)
5. (Optional) SQLTools for SQL editing in VSCode

## 📓 Step 6: Create and Run a Jupyter Notebook
In VSCode:

1. Open the workspace folder.
2. Create a subfolder: notebooks
3. Inside notebooks, create a new file: test.ipynb
4. Paste this into the first cell:
```
import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://trainee:trainpass@mariadb/warehouse")

# Test query (you can replace this later)
query = "SELECT 1 as example"
df = pd.read_sql(query, engine)

df
```
If prompted to install the Python kernel or interpreter, accept the prompt.

Run the cell. You should see a single row with example = 1.




