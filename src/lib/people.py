from dataclasses import dataclass


@dataclass(frozen=True)
class Person:
    pers_id: str
    first_name: str
    last_name: str
