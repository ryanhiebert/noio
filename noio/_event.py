import aiologic

from .routine import _sync_call
from .routine import routine


class Event:
    def __init__(self) -> None:
        self.__event = aiologic.Event()

    def set(self) -> None:
        self.__event.set()

    @routine
    async def wait(self) -> None:
        if _sync_call.get():
            self.__event.wait()
        else:
            await self.__event
