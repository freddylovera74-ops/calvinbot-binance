#!/bin/bash
# deploy.sh — Pull últimos cambios y reinicia los bots
# Llamado automáticamente por el cron o manualmente

set -e

BOT_DIR="/opt/calvinbot-binance"
LOG="$BOT_DIR/deploy.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] deploy.sh iniciado" >> "$LOG"

cd "$BOT_DIR"

# Guardar cualquier cambio local no commiteado (no debería haberlos en prod)
git stash --quiet 2>/dev/null || true

# Pull de main
git pull origin main >> "$LOG" 2>&1

# Reiniciar servicios si algo cambió
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Reiniciando servicios..." >> "$LOG"
systemctl restart calvinbot-executor   2>/dev/null || true
systemctl restart calvinbot-strategy   2>/dev/null || true
systemctl restart calvinbot-optimizer  2>/dev/null || true
systemctl restart calvinbot-watchdog   2>/dev/null || true
systemctl restart calvinbot-dashboard  2>/dev/null || true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] deploy.sh completado" >> "$LOG"
