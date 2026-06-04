

class Abort(Exception):
    pass

class Empty(Exception):
    pass

class Full(Exception):
    pass

class ObjectView:
    def __buffer__(self, flags, /):
        """
        Return a buffer object that exposes the underlying memory of the object.
        """

class Deque:
    def __init__(self, group_id: str, total_memory_capacity_mb: int = 16384) -> None: ...

    def put(self, data: object) -> None: ...

    def try_put(self, data: object) -> None: ...

    def get(self) -> object: ...

    def steal(self, target_longest: bool = False, target_first: bool = False) -> object: ...

    def put_buffer(self, data: object) -> None: ...

    def get_view(self) -> ObjectView: ...

    def steal_view(self, target_longest: bool = False, target_first: bool = False) -> ObjectView: ...

    def qsize(self) -> int: ...

    def empty(self) -> bool: ...

    def full(self) -> bool: ...
