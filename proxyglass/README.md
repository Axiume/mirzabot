# ProxyGlass Telegram Bot v2.1
Advanced NextProxy Telegram bot with:
- Telegram-only SOCKS5 output
- batch quantity picker + custom quantity
- three output formats
- optional health validation
- multiple NextProxy API keys with round-robin
- deduplication and bounded concurrency
- per-user daily quotas and rate limits
- persistent SQLite history
- proxy formatter
- admin controls, provider test, stats, maintenance mode
- Railway /health and /ready

Set secrets in Railway Variables; never commit BOT_TOKEN or API keys.
Mount a persistent Railway Volume at /data for durable history.
