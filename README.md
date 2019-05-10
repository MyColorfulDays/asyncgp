# asyncgp -- A database interface library based on asyncpg for GreenPlum and Python/asyncio

**asyncgp** is a database interface library based on asyncpg for
GreenPlum and Python/asyncio.

asyncgp requires Python 3.5 or later and add supported for GreenPlum versions 5.x.


## Documentation

You can refer to asyncpg document [here](https://magicstack.github.io/asyncpg/current/).


## Installation

asyncgp is available on PyPI.
Use pip to install:

```shell
$ pip install asyncgp
```
or:

```shell
$ git clone https://github.com/MyColorfulDays/asyncgp.git
$ cd asyncgp
$ python setup.py install
```

## Basic Usage

```python3

import asyncio
import asyncgp as asyncpg

async def run():
    conn = await asyncpg.connect(user='user', password='password', database='database', host='127.0.0.1')
    values = await conn.fetch('''SELECT * FROM mytable''')
    await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
```
