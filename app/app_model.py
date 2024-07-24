import psycopg2
from dataclasses import dataclass


@dataclass
class BookModel:
    first_name: str
    last_name: str
    avatar: bytes | None
    genres: list[str]
    title: str
    price: float | int
    pages: int
