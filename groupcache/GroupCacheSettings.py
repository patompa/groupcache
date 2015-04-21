#! /usr/bin/env python
class Settings:
    def __init__(self):
        self.GROUP_TTL = 300
        self.CLIENT_TTL = 300
        self.CLIENT_WAIT = 5.0
        self.EVICTION_INTERVAL = 3600.0
        self.THREADS = 500
settings = Settings()

