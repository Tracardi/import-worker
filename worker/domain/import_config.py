from typing import Optional

from pydantic import validator, BaseModel

from worker.domain.named_entity import NamedEntity


class ImportConfig(BaseModel):
    name: str
    description: Optional[str] = ""
    event_type: str
    module: str
    config: dict
    enabled: bool = True
    transitional: bool = True

    @validator("name")
    def validate_name(cls, value):
        if len(value) == 0:
            raise ValueError("Name cannot be empty.")
        return value

    @validator("event_type")
    def validate_event_type(cls, value):
        if len(value) == 0:
            raise ValueError("Event type cannot be empty.")
        return value
