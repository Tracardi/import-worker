from pydantic import BaseModel, validator
from datetime import datetime
from typing import Optional


class Task(BaseModel):
    id: str
    name: str
    timestamp: Optional[datetime]
    status: str = 'pending'
    progress: float = 0
    import_type: str = "missing"
    import_id: str
    task_id: str
    event_type: str = "missing"

    @validator("status")
    def validate_status(cls, value):
        if value not in ("pending", "running", "error", "done", "cancelled"):
            raise ValueError(f"Status must be one of: pending, running, error, done, cancelled. {value} given.")
        return value