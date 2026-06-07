#!/usr/bin/env python3
import asyncio
import aiohttp
import json

async def check_structure():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://gamma-api.polymarket.com/markets?limit=1") as resp:
            data = await resp.json()
            market = data[0] if isinstance(data, list) else data.get("data", [])[0] if data.get("data") else None
            
            if market:
                print("Market structure (first 1000 chars):\n")
                print(json.dumps(market, indent=2)[:1500])

asyncio.run(check_structure())
