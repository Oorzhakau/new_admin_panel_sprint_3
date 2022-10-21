import logging
import time
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger()


def backoff(
        exceptions: tuple,
        start_sleep_time: float = 0.1,
        factor: int = 2,
        border_sleep_time: int = 10,
        log: logging = logger
) -> Callable[..., Any]:
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания
    (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exceptions: перехватываемые исключения
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param log: объект логгирования
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as err:
                    log.exception(err)
                    t = t * factor
                    if t > border_sleep_time:
                        t = border_sleep_time
                    time.sleep(t)

        return inner

    return func_wrapper
