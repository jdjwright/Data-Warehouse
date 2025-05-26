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
