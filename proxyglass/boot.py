import asyncio, os
import app

async def wait_without_secrets():
    from aiohttp import web
    port=int(os.getenv("PORT","8080"))
    a=web.Application()
    async def health(_): return web.json_response({"ok":True,"service":"proxyglass","version":app.VERSION,"configured":False})
    async def ready(_): return web.json_response({"ready":False,"reason":"BOT_TOKEN missing","provider_keys":len(app.API_KEYS)},status=503)
    a.router.add_get("/health",health); a.router.add_get("/ready",ready)
    r=web.AppRunner(a); await r.setup(); await web.TCPSite(r,"0.0.0.0",port).start()
    print("ProxyGlass is built but waiting for BOT_TOKEN in Railway Variables.")
    await asyncio.Event().wait()

def main():
    if app.BOT_TOKEN:
        app.main()
    else:
        asyncio.run(wait_without_secrets())

if __name__=="__main__":
    main()
