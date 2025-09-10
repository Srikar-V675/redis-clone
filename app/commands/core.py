from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from ..parser import RESPSerializer
from .general import *
from .list import *

class RedisCommand(ABC):
    """Abstract Base class for Redis commands"""
    
    @abstractmethod
    def execute(self, args: List[str]) -> bytes:
        """Execute command and return RESP bytes response"""
        pass
    
    @abstractmethod
    def validate_args(self, args: List[str]) -> bool:
        """Validate command arguments"""
        pass


class RedisCommandHandler:
    """Routes to corresponding command class based on command received"""
    
    def __init__(self, db: Optional[Dict]):
        self.db = db if not None else {}
        self.commands = {
            # general commands
            "ECHO": EchoCommand(),
            "PING": PingCommand(),
            "SET": SetCommand(db=self.db),
            "GET": GetCommand(db=self.db),
            "TYPE": TypeCommand(db=self.db),
            # list commands
            "RPUSH": RPushCommand(db=self.db),
            "LPUSH": LPushCommand(db=self.db),
            "LRANGE": LRangeCommand(db=self.db),
            "LLEN": LLenCommand(db=self.db),
            "LPOP": LPopCommand(db=self.db),
            "BLPOP": BLPopCommand(db=self.db),
        }
    
    def handle_command(self, tokens: List[str]) -> bytes:
        """Identifies mapping from `commands` dict and executes the command"""
        
        if not tokens:
            return RESPSerializer.serialize_error("ERR empty command")
        
        cmd = tokens[0].upper()
        args = tokens[1:] if len(tokens) > 1 else []
        
        if cmd not in self.commands:
            return RESPSerializer.serialize_error(f"ERR unknown command - {cmd}")
        
        return self.commands[cmd].execute(args)