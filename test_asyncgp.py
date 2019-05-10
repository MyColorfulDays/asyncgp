#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: test_asyncgp.py
Author: Me
Email: my_colorful_days@163.com
Github: https://github.com/MyColorfulDays
Description: test asyncgp.
"""
import asyncio
import datetime
import io
import logging
import tempfile
import unittest

from asyncpg import compat

import asyncgp as asyncpg

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s %(module)s.py:%(lineno)s]%(levelname)s: %(message)s'
)

GREENPLUM = {
    'user': 'usernanme',
    'password': 'password',
    'host': '192.168.1.100',
    'port': '5432',
    'database': 'asyncgp'
}



class TestAsyncGP(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.run = self.loop.run_until_complete
        # self.con = self.run(asyncpg.connect(**GREENPLUM))
        self.con = self.run(asyncpg.create_pool(**GREENPLUM))

    def test_01_execute(self):
        res = self.run(self.con.execute("""
            DROP TABLE IF EXISTS "users";
        """))
        logging.info(res)
        self.assertEqual(res, "DROP TABLE", 'res not equal')

    def test_02_execute(self):
        res = self.run(self.con.execute("""
            CREATE TABLE "users"(
                "id" SERIAL PRIMARY KEY,
                "name" TEXT,
                "dob" DATE
            );
        """))
        logging.info(res)
        self.assertEqual(res, "CREATE TABLE", 'res not equal')

    def test_03_execute(self):
        res = self.run(
            self.con.execute('''
                INSERT INTO users(name, dob) VALUES($1, $2)
                ''', 'Bob', datetime.date(1984, 3, 1))
        )
        logging.info(res)
        self.assertEqual(res, "INSERT 0 1", 'res not equal')

    def test_04_fetchrow(self):
        res = self.run(
            self.con.fetchrow(
                'SELECT * FROM users WHERE name = $1', 'Bob')
        )
        logging.info(res)
        self.assertEqual(res['name'], "Bob", 'res not equal')

    def test_05_transaction(self):
        async def transaction():
            async with self.con.acquire() as con:
                return await con.execute("INSERT INTO users(name, dob) VALUES('John', '1984-03-02'::DATE)")
        res = self.run(transaction())
        logging.info(res)

    def test_06_fetch(self):
        res = self.run(
            self.con.fetch(
                'SELECT * FROM users')
        )
        logging.info(res)
        self.assertEqual(len(res), 2, 'res not equal')

    def test_07_fetchrow_array(self):
        res = self.run(
            self.con.fetchrow(
                'select array[[1,2,3],[4,5,6]] a')
        )
        logging.info(res)
        self.assertIsInstance(res['a'], list, 'res not list')

    def test_08_copy_from_table_basics(self):
        res = self.run(self.con.execute('''
            CREATE TABLE copytab(a text, "b~" text, i int);
            INSERT INTO copytab (a, "b~", i) (
                SELECT 'a' || i::text, 'b' || i::text, i
                FROM generate_series(1, 5) AS i
            );
            INSERT INTO copytab (a, "b~", i) VALUES('*', NULL, NULL);
        '''))

        try:
            f = io.BytesIO()

            # Basic functionality.
            res = self.run(self.con.copy_from_table('copytab', output=f))

            self.assertEqual(res, 'COPY 6')

            output = f.getvalue().decode().split('\n')
            logging.info(output)
            for line in output:
                self.assertIn(line, [
                    'a1\tb1\t1',
                    'a2\tb2\t2',
                    'a3\tb3\t3',
                    'a4\tb4\t4',
                    'a5\tb5\t5',
                    '*\t\\N\t\\N',
                    ''
                ])

            # Test parameters.
            self.run(self.con.execute('SET search_path=public'))

            f.seek(0)
            f.truncate()

            res = self.run(self.con.copy_from_table(
                'copytab', output=f, columns=('a', 'b~'),
                schema_name='public', format='csv',
                delimiter='|', null='n-u-l-l', header=True,
                quote='*', escape='!', force_quote=('a',)))

            output = f.getvalue().decode().split('\n')
            logging.info(output)

            for line in output:
                self.assertIn(
                    line,
                    [
                        'a|b~',
                        '*a1*|b1',
                        '*a2*|b2',
                        '*a3*|b3',
                        '*a4*|b4',
                        '*a5*|b5',
                        '*!**|n-u-l-l',
                        ''
                    ]
                )

            self.run(self.con.execute('SET search_path=public'))
        finally:
            self.run(self.con.execute('DROP TABLE public.copytab'))

    def test_09_copy_from_table_large_rows(self):
        res = self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b text);
            INSERT INTO copytab (a, b) (
                SELECT
                    repeat('a' || i::text, 500000),
                    repeat('b' || i::text, 500000)
                FROM
                    generate_series(1, 5) AS i
            );
        '''))

        try:
            f = io.BytesIO()

            # Basic functionality.
            res = self.run(self.con.copy_from_table('copytab', output=f))

            self.assertEqual(res, 'COPY 5')

            output = f.getvalue().decode().split('\n')
            # logging.info(output) # output too long
            for line in output:
                self.assertIn(
                    line,
                    [
                        'a1' * 500000 + '\t' + 'b1' * 500000,
                        'a2' * 500000 + '\t' + 'b2' * 500000,
                        'a3' * 500000 + '\t' + 'b3' * 500000,
                        'a4' * 500000 + '\t' + 'b4' * 500000,
                        'a5' * 500000 + '\t' + 'b5' * 500000,
                        ''
                    ]
                )
        finally:
            self.run(self.con.execute('DROP TABLE public.copytab'))

    def test_10_copy_from_query_basics(self):
        f = io.BytesIO()

        res = self.run(self.con.copy_from_query('''
            SELECT
                repeat('a' || i::text, 500000),
                repeat('b' || i::text, 500000)
            FROM
                generate_series(1, 5) AS i
        ''', output=f))

        self.assertEqual(res, 'COPY 5')

        output = f.getvalue().decode().split('\n')
        self.assertEqual(
            output,
            [
                'a1' * 500000 + '\t' + 'b1' * 500000,
                'a2' * 500000 + '\t' + 'b2' * 500000,
                'a3' * 500000 + '\t' + 'b3' * 500000,
                'a4' * 500000 + '\t' + 'b4' * 500000,
                'a5' * 500000 + '\t' + 'b5' * 500000,
                ''
            ]
        )

    def test_11_copy_from_query_with_args(self):
        f = io.BytesIO()

        res = self.run(self.con.copy_from_query('''
            SELECT
                i, i * 10
            FROM
                generate_series(1, 5) AS i
            WHERE
                i = $1
        ''', 3, output=f))

        self.assertEqual(res, 'COPY 1')

        output = f.getvalue().decode().split('\n')
        self.assertEqual(
            output,
            [
                '3\t30',
                ''
            ]
        )

    def test_12_copy_from_query_to_path(self):
        with tempfile.NamedTemporaryFile() as f:
            f.close()
            res = self.run(self.con.copy_from_query('''
                SELECT
                    i, i * 10
                FROM
                    generate_series(1, 5) AS i
                WHERE
                    i = $1
            ''', 3, output=f.name))

            with open(f.name, 'rb') as fr:
                output = fr.read().decode().split('\n')
                self.assertEqual(
                    output,
                    [
                        '3\t30',
                        ''
                    ]
                )

    def test_13_copy_from_query_to_path_like(self):
        with tempfile.NamedTemporaryFile() as f:
            f.close()

            class Path:

                def __init__(self, path):
                    self.path = path

                def __fspath__(self):
                    return self.path

            res = self.run(self.con.copy_from_query('''
                SELECT
                    i, i * 10
                FROM
                    generate_series(1, 5) AS i
                WHERE
                    i = $1
            ''', 3, output=Path(f.name)))

            with open(f.name, 'rb') as fr:
                output = fr.read().decode().split('\n')
                self.assertEqual(
                    output,
                    [
                        '3\t30',
                        ''
                    ]
                )

    def test_14_copy_from_query_cancellation_explicit(self):
        async def writer(data):
            # Sleeping here to simulate slow output sink to test
            # backpressure.
            await asyncio.sleep(0.5, loop=self.loop)

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer)

        task = self.loop.create_task(coro)
        self.run(asyncio.sleep(0.7, loop=self.loop))
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            self.run(task)

        self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

    def test_15_copy_from_query_cancellation_on_sink_error(self):
        async def writer(data):
            self.run(asyncio.sleep(0.05, loop=self.loop))
            raise RuntimeError('failure')

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer)

        task = self.loop.create_task(coro)

        with self.assertRaises(RuntimeError):
            self.run(task)

        self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

    def test_16_copy_from_query_cancellation_while_waiting_for_data(self):
        async def writer(data):
            pass

        coro = self.con.copy_from_query('''
            SELECT
                pg_sleep(60)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer)

        task = self.loop.create_task(coro)
        self.run(asyncio.sleep(0.7, loop=self.loop))
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            self.run(task)

        self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

    def test_17_copy_from_query_timeout_1(self):
        async def writer(data):
            await asyncio.sleep(0.05, loop=self.loop)

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer, timeout=0.10)

        task = self.loop.create_task(coro)

        with self.assertRaises(asyncio.TimeoutError):
            self.run(task)

        self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

    def test_18_copy_from_query_timeout_2(self):
        async def writer(data):
            try:
                await asyncio.sleep(10, loop=self.loop)
            except asyncio.TimeoutError:
                raise
            else:
                self.fail('TimeoutError not raised')

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer, timeout=0.10)

        task = self.loop.create_task(coro)

        with self.assertRaises(asyncio.TimeoutError):
            self.run(task)

        self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

    def test_19_copy_to_table_basics(self):
        res = self.run(self.con.execute('''
            CREATE TABLE copytab(a text, "b~" text, i int);
        '''))

        try:
            f = io.BytesIO()
            f.write(
                '\n'.join([
                    'a1\tb1\t1',
                    'a2\tb2\t2',
                    'a3\tb3\t3',
                    'a4\tb4\t4',
                    'a5\tb5\t5',
                    '*\t\\N\t\\N',
                    ''
                ]).encode('utf-8')
            )
            f.seek(0)

            res = self.run(self.con.copy_to_table('copytab', source=f))
            self.assertEqual(res, 'COPY 6')

            output = self.run(self.con.fetch("""
                SELECT * FROM copytab ORDER BY a
            """))
            self.assertEqual(
                output,
                [
                    ('*', None, None),
                    ('a1', 'b1', 1),
                    ('a2', 'b2', 2),
                    ('a3', 'b3', 3),
                    ('a4', 'b4', 4),
                    ('a5', 'b5', 5),
                ]
            )

            # Test parameters.
            self.run(self.con.execute('TRUNCATE copytab'))
            self.run(self.con.execute('SET search_path=public'))

            f.seek(0)
            f.truncate()

            f.write(
                '\n'.join([
                    'a|b~',
                    '*a1*|b1',
                    '*a2*|b2',
                    '*a3*|b3',
                    '*a4*|b4',
                    '*a5*|b5',
                    '*!**|*n-u-l-l*',
                    'n-u-l-l|bb'
                ]).encode('utf-8')
            )
            f.seek(0)

            if self.con.get_server_version() < (9, 4):
                force_null = None
                forced_null_expected = 'n-u-l-l'
            else:
                force_null = ('b~',)
                forced_null_expected = None

            res = self.run(self.con.copy_to_table(
                'copytab', source=f, columns=('a', 'b~'),
                schema_name='public', format='csv',
                delimiter='|', null='n-u-l-l', header=True,
                quote='*', escape='!', force_not_null=('a',)))

            self.assertEqual(res, 'COPY 7')

            self.run(self.con.execute('SET search_path=public'))

            output = self.run(self.con.fetch("""
                SELECT * FROM copytab ORDER BY a
            """))
            for line in output:
                logging.info(line)
                self.assertIn(
                    tuple(line),
                    [
                        ('*', forced_null_expected, None),
                        ('a1', 'b1', None),
                        ('a2', 'b2', None),
                        ('a3', 'b3', None),
                        ('a4', 'b4', None),
                        ('a5', 'b5', None),
                        ('n-u-l-l', 'bb', None),
                    ]
                )

        finally:
            self.run(self.con.execute('DROP TABLE public.copytab'))

    def test_20_copy_to_table_large_rows(self):
        res = self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        '''))

        try:
            class _Source:

                def __init__(self):
                    self.rowcount = 0

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if self.rowcount >= 100:
                        raise StopAsyncIteration
                    else:
                        self.rowcount += 1
                        return b'a1' * 500000 + b'\t' + b'b1' * 500000 + b'\n'

            res = self.run(self.con.copy_to_table('copytab', source=_Source()))

            self.assertEqual(res, 'COPY 100')

        finally:
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_21_copy_to_table_from_bytes_like(self):
        self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        '''))

        try:
            data = memoryview((b'a1' * 500 + b'\t' + b'b1' * 500 + b'\n') * 2)
            res = self.run(self.con.copy_to_table('copytab', source=data))
            self.assertEqual(res, 'COPY 2')
        finally:
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_22_copy_to_table_fail_in_source_1(self):
        self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        '''))

        try:
            class _Source:

                def __init__(self):
                    self.rowcount = 0

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    raise RuntimeError('failure in source')

            with self.assertRaisesRegex(RuntimeError, 'failure in source'):
                self.run(self.con.copy_to_table('copytab', source=_Source()))

            # Check that the protocol has recovered.
            self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

        finally:
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_23_copy_to_table_fail_in_source_2(self):
        self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        '''))

        try:
            class _Source:

                def __init__(self):
                    self.rowcount = 0

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if self.rowcount == 0:
                        self.rowcount += 1
                        return b'a\tb\n'
                    else:
                        raise RuntimeError('failure in source')

            with self.assertRaisesRegex(RuntimeError, 'failure in source'):
                self.run(self.con.copy_to_table('copytab', source=_Source()))

            # Check that the protocol has recovered.
            self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

        finally:
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_24_copy_to_table_timeout(self):
        self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        '''))

        try:
            class _Source:

                def __init__(self, loop):
                    self.rowcount = 0
                    self.loop = loop

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    self.rowcount += 1
                    await asyncio.sleep(60, loop=self.loop)
                    return b'a1' * 50 + b'\t' + b'b1' * 50 + b'\n'

            with self.assertRaises(asyncio.TimeoutError):
                self.run(self.con.copy_to_table(
                    'copytab', source=_Source(self.loop), timeout=0.10))

            # Check that the protocol has recovered.
            self.assertEqual(self.run(self.con.fetchval('SELECT 1')), 1)

        finally:
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_25_copy_records_to_table_1(self):
        self.run(self.con.execute('''
            CREATE TABLE copytab(a text, b int, c timestamptz);
        '''))

        try:
            date = datetime.datetime.now(tz=datetime.timezone.utc)
            delta = datetime.timedelta(days=1)

            records = [
                ('a-{}'.format(i), i, date + delta)
                for i in range(100)
            ]

            records.append(('a-100', None, None))

            res = self.run(self.con.copy_records_to_table(
                'copytab', records=records))

            self.assertEqual(res, 'COPY 101')

        finally:
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_26_copy_records_to_table_no_binary_codec(self):
        self.run(self.con.execute('''
            CREATE TABLE copytab(a uuid);
        '''))

        try:
            def _encoder(value):
                return value

            def _decoder(value):
                return value

            self.run(self.con.set_type_codec(
                'uuid', encoder=_encoder, decoder=_decoder,
                schema='pg_catalog', format='text'
            ))

            records = [('2975ab9a-f79c-4ab4-9be5-7bc134d952f0',)]

            with self.assertRaisesRegex(
                    asyncpg.InternalClientError, 'no binary format encoder'):
                self.run(self.con.copy_records_to_table(
                    'copytab', records=records))

        finally:
            self.run(self.con.reset_type_codec(
                'uuid', schema='pg_catalog'
            ))
            self.run(self.con.execute('DROP TABLE copytab'))

    def test_27_listen_01(self):
        async def test_listen_01():
            async with self.con.acquire() as con:

                q1 = asyncio.Queue(loop=self.loop)
                q2 = asyncio.Queue(loop=self.loop)

                def listener1(*args):
                    q1.put_nowait(args)

                def listener2(*args):
                    q2.put_nowait(args)

                await con.add_listener('test', listener1)
                await con.add_listener('test', listener2)

                await con.execute("NOTIFY test")

                self.assertEqual(
                    await q1.get(),
                    (con, con.get_server_pid(), 'test', ''))
                self.assertEqual(
                    await q2.get(),
                    (con, con.get_server_pid(), 'test', ''))

                await con.remove_listener('test', listener2)

                await con.execute("NOTIFY test")

                self.assertEqual(
                    await q1.get(),
                    (con, con.get_server_pid(), 'test', ''))
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(),
                                           timeout=0.05, loop=self.loop)

                await con.reset()
                await con.remove_listener('test', listener1)
                await con.execute("NOTIFY test")

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q1.get(),
                                           timeout=0.05, loop=self.loop)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(),
                                           timeout=0.05, loop=self.loop)
        self.run(test_listen_01())

    def tearDown(self):
        self.run(self.con.close())


if __name__ == '__main__':
    unittest.main()
