import asyncio, html, logging, os, sqlite3, time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse
import httpx
from aiohttp import web
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

VERSION="2.1.0"
BOT_TOKEN=os.getenv("BOT_TOKEN","").strip()
API_URL=os.getenv("NEXT_PROXY_API_URL","https://console.nextproxy.site/api/random").strip()
API_KEYS=[x.strip() for x in os.getenv("NEXT_PROXY_API_KEYS",os.getenv("NEXT_PROXY_API_KEY","")).split(",") if x.strip()]
ADMINS={int(x) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip().isdigit()}
PORT=int(os.getenv("PORT","8080")); DATA=Path(os.getenv("DATA_DIR","/data")); DATA.mkdir(parents=True,exist_ok=True)
MAX_Q=max(1,int(os.getenv("MAX_QUANTITY","50"))); DAILY=max(0,int(os.getenv("DAILY_LIMIT","100")))
RATE=max(0,float(os.getenv("USER_RATE_SECONDS","6"))); CONC=max(1,int(os.getenv("FETCH_CONCURRENCY","6")))
TG_MULT=max(1,int(os.getenv("TELEGRAM_ATTEMPTS_MULTIPLIER","6"))); MAX_ATTEMPTS=max(1,int(os.getenv("MAX_PROVIDER_ATTEMPTS","300")))
VALIDATE_DEFAULT=os.getenv("VALIDATE_DEFAULT","false").lower() in {"1","true","yes","on"}
VAL_CONC=max(1,int(os.getenv("VALIDATE_CONCURRENCY","12"))); REQ_TIMEOUT=float(os.getenv("REQUEST_TIMEOUT","18")); VAL_TIMEOUT=float(os.getenv("VALIDATE_TIMEOUT","8"))
VAL_URL=os.getenv("VALIDATE_URL","https://api.ipify.org?format=json"); PUBLIC_STATS=os.getenv("PUBLIC_STATS","false").lower() in {"1","true","yes","on"}
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"); log=logging.getLogger("proxyglass")
db=sqlite3.connect(DATA/"proxyglass.sqlite3",check_same_thread=False); db.row_factory=sqlite3.Row; db.execute("PRAGMA journal_mode=WAL")
db.executescript("""CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY,username TEXT,first_name TEXT,created_at TEXT,last_seen TEXT,daily_count INTEGER DEFAULT 0,daily_date TEXT);
CREATE TABLE IF NOT EXISTS history(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,proxy TEXT,proxy_type TEXT,mode TEXT,format_key TEXT,created_at TEXT);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);"""); db.commit()
LOCK=asyncio.Lock(); LAST={}; KEY_INDEX=0; KEY_LOCK=asyncio.Lock(); START=time.monotonic()
FORMATS={"raw":"IP:PORT","auth":"IP:PORT:USERNAME:PASSWORD","uri":"SCHEME://[USER:PASS@]IP:PORT"}

def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")
def today(): return datetime.now(timezone.utc).date().isoformat()
async def q(sql,p=(),fetch=None):
    async with LOCK:
        c=db.execute(sql,p); r=c.fetchone() if fetch=="one" else c.fetchall() if fetch=="all" else None; db.commit(); return r
async def touch(u):
    t=today(); row=await q("SELECT user_id FROM users WHERE user_id=?",(u.id,),"one")
    if row: await q("UPDATE users SET username=?,first_name=?,last_seen=?,daily_count=CASE WHEN daily_date=? THEN daily_count ELSE 0 END,daily_date=? WHERE user_id=?",(u.username or "",u.first_name or "",now(),t,t,u.id))
    else: await q("INSERT INTO users VALUES(?,?,?,?,?,?,?)",(u.id,u.username or "",u.first_name or "",now(),now(),0,t))
async def setting(k,d=""): r=await q("SELECT value FROM settings WHERE key=?",(k,),"one"); return r["value"] if r else d
async def set_setting(k,v): await q("INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(k,v))
async def maintenance(): return await setting("maintenance","0")=="1"
async def quota(uid,n):
    left=RATE-(time.monotonic()-LAST.get(uid,0))
    if left>0 and uid not in ADMINS: return False,f"⏱️ {left:.1f} ثانیه دیگر صبر کن."
    r=await q("SELECT daily_count,daily_date FROM users WHERE user_id=?",(uid,),"one"); used=r["daily_count"] if r and r["daily_date"]==today() else 0
    if DAILY and uid not in ADMINS and used+n>DAILY: return False,f"🚦 سهم روزانه: {used}/{DAILY}"
    LAST[uid]=time.monotonic(); return True,""
async def inc(uid,n): await q("UPDATE users SET daily_count=CASE WHEN daily_date=? THEN daily_count+? ELSE ? END,daily_date=? WHERE user_id=?",(today(),n,n,today(),uid))
async def save(uid,nodes,fmt,mode):
    await q("BEGIN")
    db.executemany("INSERT INTO history(user_id,proxy,proxy_type,mode,format_key,created_at) VALUES(?,?,?,?,?,?)",[(uid,fmt_node(x,fmt),x.kind,mode,fmt,now()) for x in nodes]); db.commit()

@dataclass(frozen=True)
class Node:
    ip:str; port:str; kind:str="http"; user:str=""; password:str=""
    @property
    def host(self): return f"{self.ip}:{self.port}"
    @property
    def url(self):
        scheme=norm(self.kind); host=self.ip
        if ":" in host and not host.startswith("["): host=f"[{host}]"
        auth=f"{quote(self.user,safe='')}:{quote(self.password,safe='')}@" if self.user else ""
        return f"{scheme}://{auth}{host}:{self.port}"
def norm(v):
    v=str(v or "http").strip().lower(); return {"socks":"socks5","socks5h":"socks5","https-proxy":"https"}.get(v,v)
def parse_node(d):
    ip=str(d.get("ip") or d.get("host") or d.get("server") or "").strip(); port=str(d.get("port") or "").strip()
    if not ip or not port: raise ValueError("incomplete provider node")
    return Node(ip,port,norm(d.get("type") or d.get("protocol") or d.get("scheme") or "http"),str(d.get("username") or d.get("user") or "").strip(),str(d.get("password") or d.get("pass") or "").strip())
async def key():
    global KEY_INDEX
    if not API_KEYS:return ""
    async with KEY_LOCK: k=API_KEYS[KEY_INDEX%len(API_KEYS)]; KEY_INDEX+=1; return k
async def fetch_one(c):
    k=await key()
    if not k:return None,"API key missing",None
    try:
        r=await c.get(API_URL,headers={"X-API-Key":k,"Accept":"application/json"},timeout=REQ_TIMEOUT)
        credit=r.headers.get("X-Credits-Remaining")
        if r.status_code!=200:return None,f"provider HTTP {r.status_code}",credit
        return parse_node(r.json()),None,credit
    except Exception as e:return None,str(e),None
async def fetch(n,tg=False):
    attempts=min(MAX_ATTEMPTS,n*(TG_MULT if tg else 1)); out=[]; seen=set(); errs=[]; credit=None; sem=asyncio.Semaphore(CONC)
    async with httpx.AsyncClient(follow_redirects=True) as c:
        async def w():
            async with sem:return await fetch_one(c)
        tasks=[asyncio.create_task(w()) for _ in range(attempts)]
        try:
            for fut in asyncio.as_completed(tasks):
                node,err,cr=await fut
                if cr is not None:credit=cr
                if err:errs.append(err); continue
                if not node or node.host in seen:continue
                if tg and norm(node.kind)!="socks5":continue
                seen.add(node.host); out.append(node)
                if len(out)>=n:break
        finally:
            for t in tasks:
                if not t.done(): t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
    return out,errs[:6],credit,attempts
async def valid(node):
    try:
        async with httpx.AsyncClient(proxy=node.url,timeout=VAL_TIMEOUT) as c:return 200<= (await c.get(VAL_URL)).status_code <300
    except:return False
def fmt_node(n,f):
    if f=="auth" and n.user:return f"{n.ip}:{n.port}:{n.user}:{n.password}"
    return n.url if f=="uri" else n.host
def parse_proxy(s):
    s=s.strip()
    try:
        if "://" in s:
            p=urlparse(s); return Node(p.hostname,str(p.port),norm(p.scheme),p.username or "",p.password or "") if p.hostname and p.port else None
        a=s.split(":"); return Node(a[0],a[1],user=a[2],password=a[3]) if len(a)==4 else Node(a[0],a[1]) if len(a)==2 else None
    except:return None

def main_menu(uid):
    rows=[[InlineKeyboardButton("🧊 پروکسی سریع",callback_data="g1"),InlineKeyboardButton("📱 Telegram SOCKS5",callback_data="tg")],
          [InlineKeyboardButton("📦 چندتایی",callback_data="batch"),InlineKeyboardButton("🎛 فرمت",callback_data="fmt")],
          [InlineKeyboardButton("🧪 تست سلامت",callback_data="val"),InlineKeyboardButton("🧰 فرمت‌کننده",callback_data="tool")],
          [InlineKeyboardButton("📚 تاریخچه",callback_data="hist"),InlineKeyboardButton("📡 وضعیت API",callback_data="status")],
          [InlineKeyboardButton("ℹ️ راهنما",callback_data="help")]]
    if uid in ADMINS: rows.append([InlineKeyboardButton("👑 مدیریت",callback_data="admin")])
    return InlineKeyboardMarkup(rows)
def qty(prefix):
    vals=[x for x in (1,5,10,20,50) if x<=MAX_Q]; rows=[[InlineKeyboardButton(str(x),callback_data=f"{prefix}:{x}") for x in vals],[InlineKeyboardButton("⌨️ تعداد دلخواه",callback_data=f"{prefix}:custom")],[InlineKeyboardButton("↩️ بازگشت",callback_data="home")]]; return InlineKeyboardMarkup(rows)
def fmt_menu(): return InlineKeyboardMarkup([[InlineKeyboardButton("IP:PORT",callback_data="f:raw")],[InlineKeyboardButton("IP:PORT:USER:PASS",callback_data="f:auth")],[InlineKeyboardButton("SCHEME://USER:PASS@IP:PORT",callback_data="f:uri")],[InlineKeyboardButton("↩️",callback_data="home")]])
def admin_menu(): return InlineKeyboardMarkup([[InlineKeyboardButton("📊 آمار",callback_data="a:stats"),InlineKeyboardButton("🩺 تست",callback_data="a:test")],[InlineKeyboardButton("🗃 آخرین‌ها",callback_data="a:recent"),InlineKeyboardButton("🛠 نگهداری",callback_data="a:maint")],[InlineKeyboardButton("🔐 سیستم",callback_data="a:system")],[InlineKeyboardButton("↩️",callback_data="home")]])
async def edit(u,text,markup=None):
    q=u.callback_query
    if q:
        try: await q.edit_message_text(text,parse_mode=ParseMode.HTML,reply_markup=markup); return
        except: pass
    await u.effective_message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=markup)
async def start(u,c): await touch(u.effective_user); c.user_data.setdefault("fmt","raw"); c.user_data.setdefault("val",VALIDATE_DEFAULT); await u.message.reply_text(f"🧊 <b>ProxyGlass {VERSION}</b>\nربات پیشرفته دریافت Proxy با خروجی Telegram/SOCKS5، Batch، Validation و Formatter.",parse_mode=ParseMode.HTML,reply_markup=main_menu(u.effective_user.id))
async def generate(u,c,tg,n):
    uid=u.effective_user.id
    if await maintenance() and uid not in ADMINS: return await edit(u,"🛠 ربات موقتاً در حالت نگهداری است.",main_menu(uid))
    n=max(1,min(int(n),MAX_Q)); ok,msg=await quota(uid,n)
    if not ok:return await edit(u,msg,main_menu(uid))
    await edit(u,"⏳ در حال دریافت خروجی...",None); started=time.monotonic()
    nodes,errs,credit,attempts=await fetch(n,tg)
    if c.user_data.get("val",VALIDATE_DEFAULT) and nodes:
        sem=asyncio.Semaphore(VAL_CONC)
        async def ck(x):
            async with sem:return await valid(x)
        marks=await asyncio.gather(*(ck(x) for x in nodes)); nodes=[x for x,m in zip(nodes,marks) if m]
    if not nodes:return await edit(u,"❌ خروجی مناسب پیدا نشد.\n"+"<code>"+html.escape(errs[0])+"</code>" if errs else "❌ خروجی مناسب پیدا نشد.",main_menu(uid))
    fmt=c.user_data.get("fmt","raw"); await inc(uid,len(nodes)); await save(uid,nodes,fmt,"telegram" if tg else "proxy"); ms=(time.monotonic()-started)*1000
    head=f"{'📱 Telegram / SOCKS5' if tg else '🧊 Proxy'}\n✅ <b>{len(nodes)}</b> خروجی · ⚡ <b>{ms:.0f}ms</b> · attempts <b>{attempts}</b>"
    if credit is not None:head+=f" · credits <b>{html.escape(credit)}</b>"
    lines=[fmt_node(x,fmt) for x in nodes]; body=head+"\n\n<pre>"+html.escape("\n".join(lines))+"</pre>"
    if len(body)<3600: await edit(u,body,main_menu(uid))
    else:
        await edit(u,head+"\n📄 خروجی کامل به‌صورت فایل ارسال می‌شود.",main_menu(uid)); await u.effective_message.reply_document(InputFile(("\n".join(lines)+"\n").encode(),filename="proxyglass.txt"),caption="📦 ProxyGlass export")
async def callback(u,c):
    await touch(u.effective_user); q=u.callback_query; d=q.data or ""; await q.answer()
    c.user_data.setdefault("fmt","raw"); c.user_data.setdefault("val",VALIDATE_DEFAULT); uid=u.effective_user.id
    if d=="home": c.user_data["mode"]=None; return await edit(u,"🧊 <b>ProxyGlass</b>\nانتخاب کن:",main_menu(uid))
    if d=="g1": return await generate(u,c,False,1)
    if d=="tg": return await edit(u,"📱 <b>Telegram SOCKS5</b>\nتعداد را انتخاب کن:",qty("tgq"))
    if d=="batch": return await edit(u,"📦 تعداد خروجی:",qty("bq"))
    if d.startswith("bq:") or d.startswith("tgq:"):
        p,n=d.split(":",1); tg=p=="tgq"
        if n=="custom": c.user_data["mode"]="ctg" if tg else "cb"; return await edit(u,f"⌨️ عدد بین 1 تا {MAX_Q} بفرست.",None)
        return await generate(u,c,tg,int(n))
    if d=="fmt": return await edit(u,f"🎛 فرمت فعلی: <b>{FORMATS[c.user_data['fmt']]}</b>",fmt_menu())
    if d.startswith("f:"): c.user_data["fmt"]=d[2:]; return await edit(u,f"✅ {FORMATS[c.user_data['fmt']]}",main_menu(uid))
    if d=="val": c.user_data["val"]=not bool(c.user_data["val"]); return await edit(u,f"🧪 Validation: <b>{'ON ✅' if c.user_data['val'] else 'OFF ⛔'}</b>",main_menu(uid))
    if d=="tool": c.user_data["mode"]="formatter"; return await edit(u,"🧰 یک Proxy بفرست، مثل <code>1.2.3.4:8080</code> یا <code>socks5://u:p@1.2.3.4:1080</code>.",None)
    if d=="hist":
        r=await qdb("SELECT proxy,mode,created_at FROM history WHERE user_id=? ORDER BY id DESC LIMIT 20",(uid,)); text="<b>📚 History</b>\n<pre>"+html.escape("\n".join(f"{x['proxy']} [{x['mode']}]" for x in r) or "—")+"</pre>"; return await edit(u,text,main_menu(uid))
    if d=="status":
        try:
            async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as cl:
                s=time.monotonic(); node,err,credit=await fetch_one(cl); ms=(time.monotonic()-s)*1000
            return await edit(u,f"📡 <b>NextProxy</b>\nResult: <b>{'OK ✅' if node else 'FAIL ❌'}</b>\nLatency: <b>{ms:.0f}ms</b>\nKeys: <b>{len(API_KEYS)}</b>\nCredits: <b>{html.escape(credit or '—')}</b>",main_menu(uid))
        except Exception as e:return await edit(u,f"❌ <code>{html.escape(str(e)[:220])}</code>",main_menu(uid))
    if d=="help": return await edit(u,"<b>راهنما</b>\n\n📱 Telegram mode فقط SOCKS5 واقعی Provider را خروجی می‌دهد.\n📦 Batch برای تعدادهای بزرگ است.\n🧪 Validation زمان و مصرف شبکه بیشتری دارد.\n🧰 Formatter یک Proxy موجود را به فرمت‌های مختلف تبدیل می‌کند.",main_menu(uid))
    if d=="admin" and uid in ADMINS:return await edit(u,"👑 <b>Admin</b>",admin_menu())
    if d.startswith("a:") and uid in ADMINS:
        if d=="a:stats":
            a=await qdb("SELECT COUNT(*) c FROM users",()); h=await qdb("SELECT COUNT(*) c FROM history",()); return await edit(u,f"📊 Users: <b>{a[0]['c']}</b>\nHistory: <b>{h[0]['c']}</b>\nDaily: <b>{DAILY or '∞'}</b>\nMax batch: <b>{MAX_Q}</b>",admin_menu())
        if d=="a:test":
            async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as cl:s=time.monotonic(); node,err,credit=await fetch_one(cl); ms=(time.monotonic()-s)*1000
            return await edit(u,f"🩺 Provider: <b>{'OK ✅' if node else 'FAIL ❌'}</b> · {ms:.0f}ms · Credits {html.escape(credit or '—')}",admin_menu())
        if d=="a:recent":
            r=await qdb("SELECT user_id,proxy,mode FROM history ORDER BY id DESC LIMIT 12",()); return await edit(u,"🗃 <pre>"+html.escape("\n".join(f"{x['user_id']} | {x['mode']} | {x['proxy']}" for x in r) or "—")+"</pre>",admin_menu())
        if d=="a:maint":
            nv="0" if await maintenance() else "1"; await set_setting("maintenance",nv); return await edit(u,f"🛠 Maintenance: <b>{'ON 🔴' if nv=='1' else 'OFF 🟢'}</b>",admin_menu())
        if d=="a:system": return await edit(u,f"🔐 Version <b>{VERSION}</b> · uptime <b>{int(time.monotonic()-START)}s</b> · keys <b>{len(API_KEYS)}</b> · concurrency <b>{CONC}</b>",admin_menu())
async def qdb(sql,p=()):
    async with LOCK:
        c=db.execute(sql,p); r=c.fetchall(); return r
async def text_input(u,c):
    if not u.message or not u.message.text:return
    mode=c.user_data.get("mode")
    if mode in {"cb","ctg"}:
        try:n=int(u.message.text.strip())
        except:return await u.message.reply_text("❌ فقط عدد.")
        if not 1<=n<=MAX_Q:return await u.message.reply_text(f"❌ بین 1 و {MAX_Q}.")
        c.user_data["mode"]=None; return await generate(u,c,mode=="ctg",n)
    if mode=="formatter":
        node=parse_proxy(u.message.text)
        if not node:return await u.message.reply_text("❌ فرمت Proxy تشخیص داده نشد.")
        c.user_data["mode"]=None; await u.message.reply_text(f"<b>🧰 Result</b>\n\n<code>{html.escape(node.host)}</code>\n<code>{html.escape(node.user and f'{node.ip}:{node.port}:{node.user}:{node.password}' or '(credentialless)')}</code>\n<code>{html.escape(node.url)}</code>",parse_mode=ParseMode.HTML,reply_markup=main_menu(u.effective_user.id))
async def admin(u,c):
    if u.effective_user.id not in ADMINS:return await u.message.reply_text("⛔")
    await u.message.reply_text("👑 <b>Admin</b>",parse_mode=ParseMode.HTML,reply_markup=admin_menu())
async def ping(u,c): await u.message.reply_text(f"🏓 Pong · {VERSION} · {int(time.monotonic()-START)}s")
async def health(app):
    if not web:return
    a=web.Application()
    async def h(_):return web.json_response({"ok":True,"service":"proxyglass","version":VERSION})
    async def ready(_):
        ok=bool(BOT_TOKEN and API_KEYS); return web.json_response({"ready":ok,"keys":len(API_KEYS)},status=200 if ok else 503)
    async def stats(_):
        if not PUBLIC_STATS:return web.json_response({"ok":True})
        u=await qdb("SELECT COUNT(*) c FROM users"); h=await qdb("SELECT COUNT(*) c FROM history"); return web.json_response({"ok":True,"users":u[0]["c"],"history":h[0]["c"]})
    a.router.add_get("/health",h); a.router.add_get("/ready",ready); a.router.add_get("/stats",stats); r=web.AppRunner(a); await r.setup(); await web.TCPSite(r,"0.0.0.0",PORT).start(); app.bot_data["runner"]=r
async def post_init(app): await health(app)
async def post_shutdown(app):
    r=app.bot_data.get("runner")
    if r: await r.cleanup()
    db.commit(); db.close()
def main():
    if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN missing")
    app=(ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build())
    app.add_handler(CommandHandler("start",start)); app.add_handler(CommandHandler("help",start)); app.add_handler(CommandHandler("admin",admin)); app.add_handler(CommandHandler("ping",ping))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,text_input)); app.add_handler(CallbackQueryHandler(callback)); app.run_polling(drop_pending_updates=True,allowed_updates=Update.ALL_TYPES)
if __name__=="__main__": main()
