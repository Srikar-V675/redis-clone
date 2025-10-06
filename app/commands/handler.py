from typing import List, Dict, Optional
from app.parser import RESPSerializer
from app.commands.general import *
from app.commands.list import *
from app.commands.stream import *


class RedisCommandHandler:
    """Routes to corresponding command class based on command received"""
    
    def __init__(self, db: Optional[Dict], waiting_clients: Dict):
        self.db = db if not None else {}
        self.waiting_clients = waiting_clients
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
            # stream commands
            "XADD": XAddCommand(db=self.db, waiting_clients=self.waiting_clients),
            "XRANGE": XRangeCommand(db=self.db, waiting_clients=self.waiting_clients),
            "XREAD": XreadStreamsCommand(db=self.db, waiting_clients=self.waiting_clients),
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