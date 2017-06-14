# -*- coding: utf-8 -*-
import asyncio, loggin
import aiomysql

async def create_pool(loop, **kw):
	logging.info('create database connection pool')
	global __pool
	__pool = yield from aiomysql.create_pool(
		host=kw.get('host', 'localhost'),
		port=kw.get('port', 3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 10),
		loop=loop
		)

async def select(sql, args, size=None):
	log(sql, args)
	global __pool
	with (yield from __pool) as conn:
		try:
			cur = yield from conn.cursor(aiomysql.DictCursor)
			yield from cur.execute(sql.replace('?', '%s'), args or ())
			affected = cur.rowcount
			yield from cur.close()
		except BaseException as e:
			raise
		return affected