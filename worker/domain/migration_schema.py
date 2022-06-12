from typing import Optional
from worker.domain.entity import Entity


class MigrationSchema(Entity):
    index: str
    multi: bool
    script: Optional[str] = None
    worker: str
    asynchronous: bool
    from_index: Optional[str] = None
    to_index: Optional[str] = None
