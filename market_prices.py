#!/usr/bin/env python3
"""
Fetch BTC market data from Polymarket showing
Price To Beat (reference) and Final Price for each period
"""

import asyncio
import aiohttp
from datetime import datetime

GAMMA_API = "https://gamma-api.polymarket.com"

async def fetch_market_data():
    """Fetch Polymarket BTC market reference and final prices"""
    async with aiohttp.ClientSession() as session:
        try:
            # Get markets from the Polymarket API
            url = f"{GAMMA_API}/markets"
            params = {"limit": 200}
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    print(f"Error: API returned {resp.status}")
                    return
                
                data = await resp.json()
                all_markets = data if isinstance(data, list) else data.get("data", [])
                
                # Find BTC markets - the bot trades 'btc-updown-Xm' markets
                # Look for markets with BTC in the question or slug
                btc_markets = [
                    m for m in all_markets 
                    if isinstance(m, dict) and (
                        "btc" in m.get("slug", "").lower() or 
                        "bitcoin" in m.get("question", "").lower()
                    )
                ]
                
                if not btc_markets:
                    print("❌ No BTC markets found in API")
                    print("\nAvailable markets with 'price' in them:")
                    for m in all_markets[:10]:
                        if isinstance(m, dict):
                            slug = m.get("slug", "N/A")
                            q = m.get("question", "N/A")[:50]
                            print(f"  {slug}")
                            print(f"    Q: {q}...")
                    return
                
                print(f"✅ Found {len(btc_markets)} BTC markets\n")
                print(f"{'Time Period':<15} {'Question':<40} {'Reference':<15} {'Final':<15}")
                print("=" * 90)
                
                for market in btc_markets[:10]:  # Show first 10
                    slug = market.get("slug", "N/A")
                    question = market.get("question", "N/A")[:38]
                    end_date = market.get("endDate", "")
                    
                    # Get reference and final prices if available
                    reference_price = market.get("referencePrice", None)
                    final_price = market.get("finalPrice", None)
                    
                    # Try to get prices from outcomes
                    outcomes = market.get("outcomes", [])
                    
                    # Parse time from end_date
                    if end_date:
                        try:
                            dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                            time_str = dt.strftime('%H:%M')
                        except:
                            time_str = "N/A"
                    else:
                        time_str = "N/A"
                    
                    ref_str = f"${reference_price:.2f}" if reference_price else "N/A"
                    final_str = f"${final_price:.2f}" if final_price else "N/A"
                    
                    print(f"{time_str:<15} {question:<40} {ref_str:<15} {final_str:<15}")
        
        except asyncio.TimeoutError:
            print("❌ Error: Request timeout")
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    print("🔄 Fetching BTC market price data from Polymarket...\n")
    asyncio.run(fetch_market_data())
