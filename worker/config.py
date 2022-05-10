import os


class RedisConfig:

    def __init__(self, env):
        self.redis_host = env['REDIS_HOST'] if 'REDIS_HOST' in env else 'redis://localhost:6379'
        self.redis_password = env['REDIS_PASSWORD'] if 'REDIS_PASSWORD' in env else None

    def get_redis_with_password(self):
        if not self.redis_host.startswith('redis://'):
            self.redis_host = f"redis://{self.redis_host}"
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host[8:]}"
        return self.redis_host


redis_config = RedisConfig(os.environ)
