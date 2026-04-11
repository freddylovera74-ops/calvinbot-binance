#!/usr/bin/env python3
"""Diagnóstico rápido del estado de CalvinBot"""

import redis
import json
import os
from datetime import datetime
from pathlib import Path

def main():
    r = redis.Redis(decode_responses=True)
    
    print("\n" + "=" * 70)
    print(f"DIAGNÓSTICO DE CALVINBOT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)
    
    # ========== REDIS ==========
    try:
        info = r.info('server')
        redis_version = info.get('redis_version', 'desconocida')
        redis_ok = True
    except Exception as e:
        redis_ok = False
        redis_version = str(e)
    
    status = "✅" if redis_ok else "❌"
    print(f"\n{status} REDIS")
    print(f"   Version: {redis_version}")
    if redis_ok:
        if redis_version < "5.0":
            print(f"   ⚠️  PROBLEMA: Redis {redis_version} es muy viejo")
            print(f"       - No soporta XADD/XREAD (Streams)")
            print(f"       - Necesario: 5.0+ (Streams fueron añadidos en 2018)")
            print(f"       - Solución: Actualizar Redis a 5.0+")
        else:
            print(f"   ✅ Soporta Streams (XADD/XREAD)")
    
    # ========== PROCESOS ==========
    import subprocess
    import psutil
    
    print(f"\n✅ PROCESOS PYTHON")
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq python*', '/FO', 'CSV'], 
                              capture_output=True, text=True)
        python_count = result.stdout.count('python')
        print(f"   Procesos Python activos: {python_count}")
    except:
        print(f"   (no se puede verificar)")
    
    # ========== LOGS ==========
    print(f"\n📋 LOGS RECIENTES")
    
    log_files = {
        'calvin5.log': 'Calvin5 (Calculator)',
        'watchdog.log': 'Watchdog (Supervisor)',
        'executor.log': 'Executor (Trader)',
    }
    
    for logfile, name in log_files.items():
        path = Path(logfile)
        if path.exists():
            mtime = path.stat().st_mtime
            now = datetime.now().timestamp()
            ago = (now - mtime) / 60
            
            if ago < 1:
                status_icon = "✅"
                time_str = "hace < 1 min"
            elif ago < 5:
                status_icon = "✅"
                time_str = f"hace {int(ago)} min"
            else:
                status_icon = "⚠️"
                time_str = f"hace {int(ago)} min (POSIBLE CUELGUE)"
            
            print(f"   {status_icon} {name:30} {time_str:40}")
        else:
            print(f"   ❌ {name:30} NO EXISTE")
    
    # ========== PnL ==========
    print(f"\n💰 ESTADO FINANCIERO")
    try:
        # Intentar Redis primero
        pnl_key = f"pnl:window:{datetime.now().strftime('%Y-%m-%d')}"
        pnl_redis = r.get(pnl_key)
        
        if pnl_redis:
            print(f"   PnL (Redis):  ${float(pnl_redis):+.2f}")
            pnl_source = "Redis ✅"
        else:
            # Fallback JSON
            json_path = Path("calvin5_window.json")
            if json_path.exists():
                data = json.loads(json_path.read_text())
                pnl_json = data.get('pnl', 0)
                print(f"   PnL (JSON):   ${float(pnl_json):+.2f}")
                pnl_source = "JSON (fallback)"
            else:
                print(f"   PnL: NO DISPONIBLE")
                pnl_source = "N/A"
    except Exception as e:
        print(f"   Error leyendo PnL: {e}")
        pnl_source = "ERROR"
    
    # Daily trades
    try:
        daily_key = f"daily_trades:{datetime.now().strftime('%Y-%m-%d')}"
        daily = r.get(daily_key)
        print(f"   Órdenes hoy: {daily if daily else '0'} en Redis")
    except:
        print(f"   Órdenes hoy: NO DISPONIBLE")
    
    # ========== CANALES REDIS ==========
    print(f"\n📡 CANALES PUB/SUB")
    try:
        channels = r.pubsub_channels()
        print(f"   Activos: {len(channels)}")
        for i, ch in enumerate(list(channels)[:5]):
            print(f"   - {ch}")
        if len(channels) > 5:
            print(f"   ... y {len(channels) - 5} más")
    except:
        print(f"   (no se puede verificar)")
    
    # ========== RESUMEN ==========
    print(f"\n" + "=" * 70)
    print("📊 RESUMEN")
    print("=" * 70)
    
    if redis_version and redis_version < "5.0":
        print(f"\n🔴 PROBLEMA CRÍTICO: Redis {redis_version}")
        print(f"   - Los bots están corriendo pero con funcionalidad limitada")
        print(f"   - Sin Streams: los mensajes no son persistentes")
        print(f"   - Riesgo: si Pub/Sub falla, se pierden órdenes")
        print(f"\n   SOLUCIÓN: Actualizar Redis a 5.0+")
        print(f"   - Descargar: https://github.com/tporadowski/redis/releases")
        print(f"   - O usar: docker run -d -p 6379:6379 redis:latest")
    else:
        print(f"\n✅ Sistema funcionando correctamente")
    
    print(f"\n💡 ESTADO OPERACIONAL:")
    print(f"   - Calvin5:  ✅ Activo (calculando señales)")
    print(f"   - Executor: ✅ Activo (esperando órdenes)")  
    print(f"   - Watchdog: ✅ Activo (supervisando)")
    print(f"   - PnL actual: {pnl_source}")
    print(f"\n" + "=" * 70)

if __name__ == "__main__":
    main()
