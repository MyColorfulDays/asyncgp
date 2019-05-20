#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: asyncgp.py
Author: Me
Email: my_colorful_days@163.com
Github: https://github.com/yourname
Description: asyncgp is a asnyc package for connect GreenPlum.
"""
import asyncio
import copy
import functools
import logging


import asyncpg
import asyncpg.connection
import asyncpg.pool
import asyncpg.connect_utils
from asyncpg import utils
from asyncpg.exceptions import *  # NOQA
from asyncpg.protocol import Record  # NOQA
from asyncpg.types import *  # NOQA


class Connection(asyncpg.Connection):

    def __init__(self, *args):
        asyncpg.Connection.__init__(self, *args)
        # patch start
        # https://gpdb.docs.pivotal.io/500/ref_guide/config_params/guc-list.html#gp_recursive_cte_prototype
        # https://www.postgresql.org/docs/9.2/catalog-pg-range.html
        if self._server_version < (9, 2):
            self._intro_query = self._intro_query.replace('pg_range', '(SELECT NULL AS rngtypid,NULL AS rngsubtype)')
        # patch end

    def _get_reset_query(self):
        _reset_query = asyncpg.Connection._get_reset_query(self)
        # patch start
        # https://github.com/MagicStack/asyncpg/issues/394#issuecomment-451979304
        # https://www.postgresql.org/docs/9.0/functions-info.html
        if self._server_version < (9, 0):
            _reset_query = _reset_query.replace('pg_listening_channels()', 'pg_listener')
        # patch end
        return _reset_query

    def _format_copy_opts(self, *, format=None, oids=None, freeze=None,
                          delimiter=None, null=None, header=None, quote=None,
                          escape=None, force_quote=None, force_not_null=None,
                          force_null=None, encoding=None, csv=None, binary=None):

        # patch start
        # https://www.postgresql.org/docs/9.0/sql-copy.html
        # https://www.postgresql.org/docs/8.3/sql-copy.html
        # https://gpdb.docs.pivotal.io/500/ref_guide/sql_commands/COPY.html
        kwargs = dict(locals())
        kwargs.pop('self')
        if self._server_version >= (9, 0):
            kwargs.pop('binary')
            kwargs.pop('csv')
            return asyncpg.Connection._format_copy_opts(self, **kwargs)
        else:
            opts = []
            if (isinstance(format, str) and format.upper() == 'BINARY') or (binary is not None):
                opts.append('BINARY')
            if oids is not None:
                opts.append('OIDS')
            if delimiter is not None:
                opts.append('DELIMITER %s' % utils._quote_literal(delimiter))
            if null is not None:
                opts.append('NULL %s' % utils._quote_literal(null))
            if (isinstance(format, str) and format.upper() == 'CSV') or (csv is not None):
                opts.append('CSV')
                if header is not None:
                    opts.append("HEADER")
                if quote is not None:
                    opts.append("QUOTE %s" % utils._quote_literal(quote))
                if escape is not None:
                    opts.append("ESCAPE %s" % utils._quote_literal(escape))
                if force_quote is not None:
                    opts.append('FORCE QUOTE %s' % ', '.join(utils._quote_ident(c) for c in force_quote))
                if force_not_null is not None:
                    opts.append('FORCE NOT NULL %s' % ', '.join(utils._quote_ident(c) for c in force_not_null))

            if opts:
                return ' '.join(opts)
            else:
                return ''
        # patch end

    async def copy_records_to_table(self, table_name, *, records,
                                    columns=None, schema_name=None,
                                    timeout=None):
        """Copy a list of records to the specified table using binary COPY.

        :param str table_name:
            The name of the table to copy data to.

        :param records:
            An iterable returning row tuples to copy into the table.

        :param list columns:
            An optional list of column names to copy.

        :param str schema_name:
            An optional schema name to qualify the table.

        :param float timeout:
            Optional timeout value in seconds.

        :return: The status string of the COPY command.

        Example:

        .. code-block:: pycon

            >>> import asyncpg
            >>> import asyncio
            >>> async def run():
            ...     con = await asyncpg.connect(user='postgres')
            ...     result = await con.copy_records_to_table(
            ...         'mytable', records=[
            ...             (1, 'foo', 'bar'),
            ...             (2, 'ham', 'spam')])
            ...     print(result)
            ...
            >>> asyncio.get_event_loop().run_until_complete(run())
            'COPY 2'

        .. versionadded:: 0.11.0
        """
        tabname = utils._quote_ident(table_name)
        if schema_name:
            tabname = utils._quote_ident(schema_name) + '.' + tabname

        if columns:
            col_list = ', '.join(utils._quote_ident(c) for c in columns)
            cols = '({})'.format(col_list)
        else:
            col_list = '*'
            cols = ''

        intro_query = 'SELECT {cols} FROM {tab} LIMIT 1'.format(
            tab=tabname, cols=col_list)

        intro_ps = await self._prepare(intro_query, use_cache=True)

        # patch start
        if self.get_server_version() < (9, 0):
            opts = 'BINARY'
        else:
            opts = '(FORMAT BINARY)'
        # patch end

        copy_stmt = 'COPY {tab}{cols} FROM STDIN {opts}'.format(
            tab=tabname, cols=cols, opts=opts)

        return await self._copy_in_records(
            copy_stmt, records, intro_ps._state, timeout)


async def connect(dsn=None, *,
                  host=None, port=None,
                  user=None, password=None, passfile=None,
                  database=None,
                  loop=None,
                  timeout=60,
                  statement_cache_size=100,
                  max_cached_statement_lifetime=300,
                  max_cacheable_statement_size=1024 * 15,
                  command_timeout=None,
                  ssl=None,
                  connection_class=Connection,
                  server_settings=None):
    r"""A coroutine to establish a connection to a PostgreSQL server.

    The connection parameters may be specified either as a connection
    URI in *dsn*, or as specific keyword arguments, or both.
    If both *dsn* and keyword arguments are specified, the latter
    override the corresponding values parsed from the connection URI.
    The default values for the majority of arguments can be specified
    using `environment variables <postgres envvars>`_.

    Returns a new :class:`~asyncpg.connection.Connection` object.

    :param dsn:
        Connection arguments specified using as a single string in the
        `libpq connection URI format`_:
        ``postgres://user:password@host:port/database?option=value``.
        The following options are recognized by asyncpg: host, port,
        user, database (or dbname), password, passfile, sslmode.
        Unlike libpq, asyncpg will treat unrecognized options
        as `server settings`_ to be used for the connection.

    :param host:
        Database host address as one of the following:

        - an IP address or a domain name;
        - an absolute path to the directory containing the database
          server Unix-domain socket (not supported on Windows);
        - a sequence of any of the above, in which case the addresses
          will be tried in order, and the first successful connection
          will be returned.

        If not specified, asyncpg will try the following, in order:

        - host address(es) parsed from the *dsn* argument,
        - the value of the ``PGHOST`` environment variable,
        - on Unix, common directories used for PostgreSQL Unix-domain
          sockets: ``"/run/postgresql"``, ``"/var/run/postgresl"``,
          ``"/var/pgsql_socket"``, ``"/private/tmp"``, and ``"/tmp"``,
        - ``"localhost"``.

    :param port:
        Port number to connect to at the server host
        (or Unix-domain socket file extension).  If multiple host
        addresses were specified, this parameter may specify a
        sequence of port numbers of the same length as the host sequence,
        or it may specify a single port number to be used for all host
        addresses.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``PGPORT`` environment variable, or ``5432`` if
        neither is specified.

    :param user:
        The name of the database role used for authentication.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``PGUSER`` environment variable, or the
        operating system name of the user running the application.

    :param database:
        The name of the database to connect to.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``PGDATABASE`` environment variable, or the
        operating system name of the user running the application.

    :param password:
        Password to be used for authentication, if the server requires
        one.  If not specified, the value parsed from the *dsn* argument
        is used, or the value of the ``PGPASSWORD`` environment variable.
        Note that the use of the environment variable is discouraged as
        other users and applications may be able to read it without needing
        specific privileges.  It is recommended to use *passfile* instead.

    :param passfile:
        The name of the file used to store passwords
        (defaults to ``~/.pgpass``, or ``%APPDATA%\postgresql\pgpass.conf``
        on Windows).

    :param loop:
        An asyncio event loop instance.  If ``None``, the default
        event loop will be used.

    :param float timeout:
        Connection timeout in seconds.

    :param int statement_cache_size:
        The size of prepared statement LRU cache.  Pass ``0`` to
        disable the cache.

    :param int max_cached_statement_lifetime:
        The maximum time in seconds a prepared statement will stay
        in the cache.  Pass ``0`` to allow statements be cached
        indefinitely.

    :param int max_cacheable_statement_size:
        The maximum size of a statement that can be cached (15KiB by
        default).  Pass ``0`` to allow all statements to be cached
        regardless of their size.

    :param float command_timeout:
        The default timeout for operations on this connection
        (the default is ``None``: no timeout).

    :param ssl:
        Pass ``True`` or an `ssl.SSLContext <SSLContext_>`_ instance to
        require an SSL connection.  If ``True``, a default SSL context
        returned by `ssl.create_default_context() <create_default_context_>`_
        will be used.

    :param dict server_settings:
        An optional dict of server runtime parameters.  Refer to
        PostgreSQL documentation for
        a `list of supported options <server settings>`_.

    :param Connection connection_class:
        Class of the returned connection object.  Must be a subclass of
        :class:`~asyncpg.connection.Connection`.

    :return: A :class:`~asyncpg.connection.Connection` instance.

    Example:

    .. code-block:: pycon

        >>> import asyncpg
        >>> import asyncio
        >>> async def run():
        ...     con = await asyncpg.connect(user='postgres')
        ...     types = await con.fetch('SELECT * FROM pg_type')
        ...     print(types)
        ...
        >>> asyncio.get_event_loop().run_until_complete(run())
        [<Record typname='bool' typnamespace=11 ...

    .. versionadded:: 0.10.0
       Added ``max_cached_statement_use_count`` parameter.

    .. versionchanged:: 0.11.0
       Removed ability to pass arbitrary keyword arguments to set
       server settings.  Added a dedicated parameter ``server_settings``
       for that.

    .. versionadded:: 0.11.0
       Added ``connection_class`` parameter.

    .. versionadded:: 0.16.0
       Added ``passfile`` parameter
       (and support for password files in general).

    .. versionadded:: 0.18.0
       Added ability to specify multiple hosts in the *dsn*
       and *host* arguments.

    .. _SSLContext: https://docs.python.org/3/library/ssl.html#ssl.SSLContext
    .. _create_default_context:
        https://docs.python.org/3/library/ssl.html#ssl.create_default_context
    .. _server settings:
        https://www.postgresql.org/docs/current/static/runtime-config.html
    .. _postgres envvars:
        https://www.postgresql.org/docs/current/static/libpq-envars.html
    .. _libpq connection URI format:
        https://www.postgresql.org/docs/current/static/\
        libpq-connect.html#LIBPQ-CONNSTRING
    """
    # patch start
    if not issubclass(connection_class, asyncpg.Connection):
        raise TypeError(
            'connection_class is expected to be a subclass of '
            'asyncgp.Connection, got {!r}'.format(connection_class))
    else:
        if issubclass(connection_class, Connection):
            if server_settings is None:
                server_settings = {'gp_recursive_cte_prototype': 'ON'}
            else:
                server_settings.update({'gp_recursive_cte_prototype': 'ON'})
    # patch end

    if loop is None:
        loop = asyncio.get_event_loop()

    return await asyncpg.connect_utils._connect(
        loop=loop, timeout=timeout, connection_class=connection_class,
        dsn=dsn, host=host, port=port, user=user,
        password=password, passfile=passfile,
        ssl=ssl, database=database,
        server_settings=server_settings,
        command_timeout=command_timeout,
        statement_cache_size=statement_cache_size,
        max_cached_statement_lifetime=max_cached_statement_lifetime,
        max_cacheable_statement_size=max_cacheable_statement_size)


class Pool(asyncpg.pool.Pool):

    def __init__(self, *args, **kwargs):
        asyncpg.pool.Pool.__init__(self, *args, **kwargs)
        # patch start
        self._server_version = None
        # patch end

    def get_server_version(self):
        return self._server_version

    async def _get_new_connection(self):
        if self._working_addr is None:
            # First connection attempt on this pool.
            con = await connect(
                *self._connect_args,
                loop=self._loop,
                connection_class=self._connection_class,
                **self._connect_kwargs)

            self._working_addr = con._addr
            self._working_config = con._config
            self._working_params = con._params
            self._server_version = copy.deepcopy(con._server_version)

        else:
            # We've connected before and have a resolved address,
            # and parsed options and config.
            con = await asyncpg.connect_utils._connect_addr(
                loop=self._loop,
                addr=self._working_addr,
                timeout=self._working_params.connect_timeout,
                config=self._working_config,
                params=self._working_params,
                connection_class=self._connection_class)

        if self._init is not None:
            try:
                await self._init(con)
            except Exception as ex:
                # If a user-defined `init` function fails, we don't
                # know if the connection is safe for re-use, hence
                # we close it.  A new connection will be created
                # when `acquire` is called again.
                try:
                    # Use `close()` to close the connection gracefully.
                    # An exception in `init` isn't necessarily caused
                    # by an IO or a protocol error.  close() will
                    # do the necessary cleanup via _release_on_close().
                    await con.close()
                finally:
                    raise ex

        return con

    async def copy_from_table(self, *args, **kwargs):
        """Copy table contents to a file or file-like object.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.copy_from_table() <connection.Connection.copy_from_table>`.
        """
        async with self.acquire() as con:
            return await con.copy_from_table(*args, **kwargs)

    async def copy_from_query(self, *args, **kwargs):
        """Copy table contents to a file or file-like object.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.copy_from_query() <connection.Connection.copy_from_query>`.
        """
        async with self.acquire() as con:
            return await con.copy_from_query(*args, **kwargs)

    async def copy_to_table(self, *args, **kwargs):
        """Copy table contents to a file or file-like object.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.copy_to_table() <connection.Connection.copy_to_table>`.
        """
        async with self.acquire() as con:
            return await con.copy_to_table(*args, **kwargs)

    async def copy_records_to_table(self, *args, **kwargs):
        """Copy table contents to a file or file-like object.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.copy_records_to_table() <connection.Connection.copy_records_to_table>`.
        """
        async with self.acquire() as con:
            return await con.copy_records_to_table(*args, **kwargs)

    async def reset_type_codec(self, *args, **kwargs):
        """Copy table contents to a file or file-like object.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.reset_type_codec() <connection.Connection.reset_type_codec>`.
        """
        async with self.acquire() as con:
            return await con.reset_type_codec(*args, **kwargs)

    async def set_type_codec(self, *args, **kwargs):
        """Copy table contents to a file or file-like object.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.set_type_codec() <connection.Connection.set_type_codec>`.
        """
        async with self.acquire() as con:
            return await con.set_type_codec(*args, **kwargs)


def create_pool(dsn=None, *,
                min_size=10,
                max_size=10,
                max_queries=50000,
                max_inactive_connection_lifetime=300.0,
                setup=None,
                init=None,
                loop=None,
                connection_class=Connection,
                **connect_kwargs):
    r"""Create a connection pool.

    Can be used either with an ``async with`` block:

    .. code-block:: python

        async with asyncpg.create_pool(user='postgres',
                                       command_timeout=60) as pool:
            async with pool.acquire() as con:
                await con.fetch('SELECT 1')

    Or directly with ``await``:

    .. code-block:: python

        pool = await asyncpg.create_pool(user='postgres', command_timeout=60)
        con = await pool.acquire()
        try:
            await con.fetch('SELECT 1')
        finally:
            await pool.release(con)

    .. warning::
        Prepared statements and cursors returned by
        :meth:`Connection.prepare() <connection.Connection.prepare>` and
        :meth:`Connection.cursor() <connection.Connection.cursor>` become
        invalid once the connection is released.  Likewise, all notification
        and log listeners are removed, and ``asyncpg`` will issue a warning
        if there are any listener callbacks registered on a connection that
        is being released to the pool.

    :param str dsn:
        Connection arguments specified using as a single string in
        the following format:
        ``postgres://user:pass@host:port/database?option=value``.

    :param \*\*connect_kwargs:
        Keyword arguments for the :func:`~asyncpg.connection.connect`
        function.

    :param Connection connection_class:
        The class to use for connections.  Must be a subclass of
        :class:`~asyncpg.connection.Connection`.

    :param int min_size:
        Number of connection the pool will be initialized with.

    :param int max_size:
        Max number of connections in the pool.

    :param int max_queries:
        Number of queries after a connection is closed and replaced
        with a new connection.

    :param float max_inactive_connection_lifetime:
        Number of seconds after which inactive connections in the
        pool will be closed.  Pass ``0`` to disable this mechanism.

    :param coroutine setup:
        A coroutine to prepare a connection right before it is returned
        from :meth:`Pool.acquire() <pool.Pool.acquire>`.  An example use
        case would be to automatically set up notifications listeners for
        all connections of a pool.

    :param coroutine init:
        A coroutine to initialize a connection when it is created.
        An example use case would be to setup type codecs with
        :meth:`Connection.set_builtin_type_codec() <\
        asyncpg.connection.Connection.set_builtin_type_codec>`
        or :meth:`Connection.set_type_codec() <\
        asyncpg.connection.Connection.set_type_codec>`.

    :param loop:
        An asyncio event loop instance.  If ``None``, the default
        event loop will be used.

    :return: An instance of :class:`~asyncpg.pool.Pool`.

    .. versionchanged:: 0.10.0
       An :exc:`~asyncpg.exceptions.InterfaceError` will be raised on any
       attempted operation on a released connection.

    .. versionchanged:: 0.13.0
       An :exc:`~asyncpg.exceptions.InterfaceError` will be raised on any
       attempted operation on a prepared statement or a cursor created
       on a connection that has been released to the pool.

    .. versionchanged:: 0.13.0
       An :exc:`~asyncpg.exceptions.InterfaceWarning` will be produced
       if there are any active listeners (added via
       :meth:`Connection.add_listener() <connection.Connection.add_listener>`
       or :meth:`Connection.add_log_listener()
       <connection.Connection.add_log_listener>`) present on the connection
       at the moment of its release to the pool.
    """
    return Pool(
        dsn,
        connection_class=connection_class,
        min_size=min_size, max_size=max_size,
        max_queries=max_queries, loop=loop, setup=setup, init=init,
        max_inactive_connection_lifetime=max_inactive_connection_lifetime,
        **connect_kwargs)


__all__ = ('connect', 'create_pool', 'Record', 'Connection') + \
          asyncpg.exceptions.__all__  # NOQA
