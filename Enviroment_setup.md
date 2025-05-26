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
cd synthetic-data-warehouse'''

## 📄 Step 3: Create docker-compose.yml
Create a new file called docker-compose.yml and paste in the contents from the code block at the bottom titled docker-compose.yml.

'''
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
'''
