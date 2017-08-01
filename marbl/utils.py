import asyncio

def create_task(coro_obj):
    '''
    wrapper for creating a task that can be used for waiting
    until a task has started.

    :param coro_obj: coroutine object to schedule
    :returns: a two element tuple where the first element
        is the task object. Awaiting on this will return when
        the coroutine object is done executing. The second element
        is a future that becomes done when the coroutine object is started.

    .. note:: must only be called from within the thread
        where the event loop resides
    '''

    loop = asyncio.get_event_loop()

    async def task_wrapper(coro_obj, launched):
        launched.set_result(True)
        await coro_obj

    launched = loop.create_future()
    return loop.create_task(task_wrapper(coro_obj, launched)), launched