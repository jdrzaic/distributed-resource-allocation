from enum import Enum


class CsState(Enum):
    OUT = 0
    TRYING = 1
    IN = 2


class MsgTag(Enum):
    REQUEST = 0
    PERMISSION = 1
    NOT_USED = 2

