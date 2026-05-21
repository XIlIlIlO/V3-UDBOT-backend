from __future__ import annotations

import httpx


class TelegramNotifier:
    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def close(self) -> None:
        await self.client.aclose()

    async def send(self, text: str) -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            await self.client.post(
                url,
                json={"chat_id": self.chat_id, "text": text},
            )
        except Exception:
            return
