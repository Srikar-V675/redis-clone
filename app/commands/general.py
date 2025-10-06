from .base import RedisCommand
from typing import List, Dict
import time
from app.parser import RESPSerializer

class EchoCommand(RedisCommand):
    """Implementation of ECHO command"""
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'echo' command"
            )
        
        return RESPSerializer.serialize_bulk_string(args[0])
    
    def validate_args(self, args:List[str]) -> bool:
        return len(args) == 1


class PingCommand(RedisCommand):
    """Implementation of PING command"""
    
    def execute(self, args:List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'ping' command"
            )
        
        return RESPSerializer.serialize_simple_string("PONG")
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 0


class SetCommand(RedisCommand):
    """Implementation of SET command"""
    
    def __init__(self, db: Dict):
        self.db = db
    
    def execute(self, args:List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'set' command"
            )
        
        key, value, options = args[0], args[1], args[2:] if len(args) > 2 else ()
        
        # px -> expire option in milliseconds
        if options and options[0].upper() == "PX":
            try:
                expiry_time_ms = int(options[1])
                if expiry_time_ms <= 0:
                    raise ValueError
                
                self.db[key] = {
                    "value": value,
                    "type": "string",
                    "expiry": (
                        time.time() * 1000 + expiry_time_ms
                    )
                }
            except ValueError:
                return RESPSerializer.serialize_error(
                    "ERR value is not an integer or out of range"
                )
        else:
            self.db[key] = {
                "value": value,
                "type": "string",
                "expiry": None
            }
        return RESPSerializer.serialize_simple_string("OK")
    
    def validate_args(self, args:List[str]) -> bool:
        return len(args) >= 2


class GetCommand(RedisCommand):
    """Implementation of GET command"""
    
    def __init__(self, db: Dict):
        self.db = db
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'get' command"
            )
        
        key = args[0]
        entry = self.db.get(key)
        
        if entry is None:
            return RESPSerializer.serialize_bulk_string(None)
        
        if entry["type"] != "string":
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        if entry["expiry"] and (time.time()*1000) >= entry["expiry"]:
            del self.db[key]
            return RESPSerializer.serialize_bulk_string(None)
        
        return RESPSerializer.serialize_bulk_string(entry["value"])
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 1


class TypeCommand(RedisCommand):
    """Implementation of TYPE command"""
    
    def __init__(self, db: Dict):
        self.db = db
    
    def execute(self, args:List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'type' command"
            )
        
        key = args[0]
        entry = self.db.get(key)
        
        if entry:
            return RESPSerializer.serialize_simple_string(entry["type"])
        else:
            return RESPSerializer.serialize_simple_string("none")
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 1
