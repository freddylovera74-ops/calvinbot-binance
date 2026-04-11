#!/usr/bin/env python3
"""Test dashboard data sources"""

import asyncio
import redis.asyncio as aioredis
import json

async def test():
    r = aioredis.Redis.from_url('redis://localhost:6379', decode_responses=True)
    
    # Test 1: Obtener estado del calculador
    print('\n🔍 TEST 1: Verificar estado en Redis')
    raw = await r.get('state:calculator:latest')
    if raw:
        state = json.loads(raw)
        print('   ✅ Estado encontrado:')
        btc_price = state.get('btc_price')
        momentum = state.get('momentum')
        print(f'   - BTC Price: {btc_price}')
        print(f'   - Momentum: {momentum}')
    else:
        print('   ❌ No hay estado en Redis')
    
    # Test 2: Verificar canales
    print('\n🔍 TEST 2: Verificar canales Pub/Sub')
    channels = await r.pubsub_channels()
    print(f'   Canales activos: {len(channels)}')
    has_hb = 'health:heartbeats' in channels
    has_ex = 'execution:logs' in channels
    has_log = 'logs:unified' in channels
    print(f'   - health:heartbeats: {has_hb}')
    print(f'   - execution:logs: {has_ex}')
    print(f'   - logs:unified: {has_log}')
    
    # Test 3: Leer Paper Trading
    print('\n🔍 TEST 3: Verificar Paper Trading')
    raw_pt = await r.get('paper:trading:state')
    if raw_pt:
        pt = json.loads(raw_pt)
        print(f'   ✅ Paper Trading: {pt}')
    else:
        print('   ❌ No hay Paper Trading state')
    
    # Test 4: Leer PnL
    print('\n🔍 TEST 4: Verificar PnL')
    raw_pnl = await r.get('pnl:window:2026-03-26')
    if raw_pnl:
        print(f'   ✅ PnL: {raw_pnl}')
    else:
        print('   ❌ No hay PnL en Redis')
    
    await r.close()
    print('\n' + '=' * 60)

asyncio.run(test())
