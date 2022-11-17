import asyncio
import datetime
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from more_itertools import chunked


PG_DSN = 'postgresql+asyncpg://test:1234@127.0.0.1:5431/test_asyncio'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class HeroesModel(Base):

    __tablename__ = 'heroes'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(255), nullable=True)
    eye_color = Column(String(255), nullable=True)
    films = Column(String(255), nullable=True)
    gender = Column(String(255), nullable=True)
    hair_color = Column(String(255), nullable=True)
    height = Column(String(255), nullable=True)
    homeworld = Column(String(255), nullable=True)
    mass = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    skin_color = Column(String(255), nullable=True)
    spices = Column(String(255), nullable=True)
    starships = Column(String(255), nullable=True)
    vechicles = Column(String(255), nullable=True)


CHUNK_SIZE = 10


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer.clear()


async def get_person(people_id: int, session: ClientSession):
    async with session.get(f'https://www.swapi.tech/api/people/{people_id}') as response:
        json_data = await response.json()
        try:
            return json_data['result']['properties']
        except KeyError:
            print(people_id, json_data)


async def get_people():

    async with ClientSession() as session:
        for chunk in chunked(range(1, 80), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([HeroesModel(**item) for item in people_chunk])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))


if __name__ == '__main__':
    asyncio.run(main())
