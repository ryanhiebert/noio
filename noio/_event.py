import aiologic

from .routine import _Mode
from .routine import _mode
from .routine import routine


class Event:
    def __init__(self) -> None:
        self.__event = aiologic.Event()

    def set(self) -> None:
        self.__event.set()

    @routine
    async def wait(self) -> None:
        if _mode.get() is _Mode.DRIVEN:
            self.__event.wait()
        else:
            await self.__event
