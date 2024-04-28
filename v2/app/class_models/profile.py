from datetime import datetime
from typing import Self


class Profile:

    def __init__(self, name: str, burst_rate: int, headers: dict, cookies: dict):
        self.name = name
        self.headers = headers
        self.cookies = cookies
        self.created_at = datetime.now()
        self.crawl_history: list[datetime] = list()
        self.burst_rate = burst_rate

    def __repr__(self):
        return self.name

    def migrate_profile(self, profile: Self) -> None:
        self.headers = profile.headers
        self.cookies = profile.cookies
        self.burst_rate = profile.burst_rate