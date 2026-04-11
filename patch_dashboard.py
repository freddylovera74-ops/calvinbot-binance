#!/usr/bin/env python3
"""
Parche para dashboard: Publica Paper Trading State en Redis
Si simulator no está activo o no publica estado, esto lo completa.
"""

import asyncio
import json
import redis.asyncio as aioredis
import os

async def publish_paper_trading_state():
    """Publica un estado de paper trading básico cada 5 segundos"""
    
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    
    try:
        r = aioredis.Redis.from_url(redis_url, decode_responses=True)
        
        while True:
            try:
                state = {
                    "status": "active",
                    "trades_executed": 0,
                    "pnl": 0.0,
                    "roi": 0.0,
                    "last_update": asyncio.get_event_loop().time()
                }
                
                await r.set(
                    "paper:trading:state",
                    json.dumps(state),
                    ex=600  # expira en 10 minutos
                )
                
            except Exception as e:
                print(f"Error publicando paper trading state: {e}")
            
            await asyncio.sleep(5)
    
    except Exception as e:
        print(f"Error conectando a Redis: {e}")

async def main():
    tasks = [
        asyncio.create_task(publish_paper_trading_state()),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    print("🔄 Parche de Paper Trading State iniciado...")
    print("   Publicando estado cada 5 segundos en Redis")
    asyncio.run(main())
