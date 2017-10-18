---
layout: post
title: "Python SQl DB module"
author: "Ye Sheng"
---

每当有朋友要学习python时，我都会像他/她推荐[廖雪峰的python教程](https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000)。这个教程内容全面，讲解思路清晰，无论是刚接触python的初学者还是已经对python有一定经验的程序员，都能<!--break-->从中学到不少东西。像我个人接触python也有几年时间了，时不时还是会去参考一下这个教程。

教程前半部分的python知识介绍写的很清晰，读起来基本上不会有什么困难。但是最后部分的python实战编写web app部分，对于代码的详细解释比较少。对于刚接触python的人不是很友好，我最近正好又重新温习一遍这部分教程，干脆做一些代码注解，希望对自己和他人都有帮助。

先附上该project的[github repo(python 2.7)](https://github.com/michaelliao/awesome-python-webapp)

今天主要关注的是第一天的数据库模块(`www/transwarp/db.py`)。该模块通过python的mysql connector从比较底层(对于一个web framework来说)的实现了数据库sql query的封装。

## 正文

最一开始是一些helper function和一个改进python `dict`的`Dict` class,这部分没什么难度就直接跳过了。真正比较重要的部分从`_LasyConnection`开始.

### _LasyConnection

```python
class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        # cursor是对数据库进行sql query的操作者，需要操作时检查是否已经initialize connection
        if self.connection is None:
            # 全局有一个DB engine,需要对数据库操作时从engine获得一个数据库connection
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        # 结束connection
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()        
```

几年前第一次读这个DB模块时，被一上来定义的各种class搞昏，对他们之间的关系似懂非懂，大概明白功能是如何实现的却不知道为啥要定义这么多环环相扣的class。这次再读，对他们之间的关系算是有了比较清楚的认识,各个class确实都有其存在的意义。
这里的`_LasyConnection`class是整个模块最底层对mysql connection的封装。几个方法都直接调用了mysql connection的操作，其存在周期略长于真正的connection(init时并没有init对应的connection)。


### _DbCtx

``` python
class _DbCtx(threading.local):
    '''
    Thread local object that holds connection info.
    '''
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        '''
        Return cursor
        '''
        return self.connection.cursor()
```

`DbCtx`对`LasyConnection`进行了进一步的封装，因为是一个`ThreadingLocal`所以各个线程会拿到不同的`DbCtx.connection`，不会互相干扰。如果不需要支持multi threading的话，`DbCtx`和`LasyConnection`完全可以合并成一个class。另外这里廖雪峰选择在每次`cleanup`时销毁之前的`LasyConnection` instance，并不是完全必要的，因为在`LasyConnection` cleanup时已经对真正的数据库connection进行了关闭。不过像他这样做也许逻辑会更清晰些。

`_db_ctx = _DbCtx()`是一个global vairable,任何进程下均可拿到`_db_ctx`，通过其则能拿到具体的链接。 至此，基本的数据库连接的封装已经完成，可以通过

```python
_db_ctx.init()
do_some_updates_select_or_whatever()
_db_ctx.cleanup()
```
来进行基本的操作。后面的代码则主要是进行一些更高级的封装，实现`with connection()`, transaction以及具体select,update等的支持。

### Engine 和 craete_engine()

```python
class _Engine(object):

    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda: mysql.connector.connect(**params))
    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

```
这部分做的事情很简单，`engine.connect`可以理解以为是一个configure好的返回具体connection的function。这里廖雪峰又通过`connect`对`_connect`进行wrap，意义不明。直接`self.connect = connect`效果是一样的。

### _ConnectionCtx

```python
class _ConnectionCtx(object):
    '''
    _ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most 
    outer connection has effect.
    with connection():
        pass
        with connection():
            pass
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    '''
    Return _ConnectionCtx object that can be used by 'with' statement:
    with connection():
        pass
    '''
    return _ConnectionCtx()

def with_connection(func):
    '''
    Decorator for reuse connection.
    @with_connection
    def foo(*args, **kw):
        f1()
        f2()
        f3()
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper
```
这部分主要是提供了with connection和对应的decorator。通过entry时检测connection是否存在来判断是否为nested connection。在exit时只有最外层的connection会cleanup。这样就保证了无论nest多少层，都使用的是全局的同一个connection。


### Transaction

```python
class _TransactionCtx(object):
    '''
    _TransactionCtx object that can handle transactions.
    with _TransactionCtx():
        pass
    '''

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            # needs open a connection first:
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')

def transaction():
    return _TransactionCtx()

def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper
```

采用类似之前connection的方法来构建transaction的支持。唯一不同的是，这里通过一个`_db_ctx.transaction`来记录当前的transaction层数，因为`_db_ctx`本身又是个`ThredingLocal`所以transaction计数是线程安全的。整个transaction层数0结束后，成功则commit，失败就rollback。

剩下的代码就是使用写好的DB链接utility来具体执行sql query们主要分为select和update两类。update类会在结束后检测是否在transaction中，如不在则自动commit。这里我觉得是不是直接用个transaction的decorator来的省事些，

写完db部分的utility后，教程的下一节就是编写ORM，通过更为抽象的数据库模型来对数据库进行操作。
