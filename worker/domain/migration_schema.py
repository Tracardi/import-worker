from typing import Optional
from worker.domain.named_entity import NamedEntity


class MigrationSchema(NamedEntity):
    multi: bool
    script: Optional[str] = None
    worker: str
    asynchronous: bool
    from_index: Optional[str] = None
    to_index: Optional[str] = None
