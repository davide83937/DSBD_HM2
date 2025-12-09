from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SendEmailMessage(_message.Message):
    __slots__ = ("email",)
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    email: str
    def __init__(self, email: _Optional[str] = ...) -> None: ...

class UserCheckMessage(_message.Message):
    __slots__ = ("email", "token")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    email: str
    token: str
    def __init__(self, email: _Optional[str] = ..., token: _Optional[str] = ...) -> None: ...

class UserResponse(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: int
    message: str
    def __init__(self, status: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...
