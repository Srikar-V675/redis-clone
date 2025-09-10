from .core import RedisCommand
from typing import List, Dict
from ..parser import RESPSerializer
from collections import deque
from threading import Event

class RedisListCommandBase(RedisCommand):
    """Base class with common functionality for list commands"""
    
    def __init__(self, db: Dict):
        self.db = db
    
    def ensure_list_type(self, entry: Dict) -> bool:
        """Ensure whether accessed key is list type"""
        return entry["type"] == "list"
    
    def cleanup_empty_key(self, key: str, entry: Dict):
        """Cleanup key with empty value"""
        if not entry["value"]:
            del self.db[key]


class RPushCommand(RedisListCommandBase):
    """Implementation of RPUSH Command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) >= 2
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'rpush' command"
            )
        
        key, values = args[0], args[1:]
        
        entry = self.db.get(key)
        
        if not entry:
            self.db[key] = {
                "value": deque([]),
                "type": "list",
                "blocking_clients": deque([])
            }
            entry = self.db[key]
        
        if not self.ensure_list_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        start = 0
        for val in values:
            if not entry["blocking_clients"]:
                break
            client = entry["blocking_clients"].popleft()
            client["value"] = val
            client["event"].set()
            start += 1
        if start < len(values):
            entry["value"].extend(values[start:])
        
        return RESPSerializer.serialize_integer(len(entry["value"]))


class LPushCommand(RedisListCommandBase):
    """Implementation of LPUSH command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) >= 2
    
    def execute(self, args:List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'lpush' command"
            )
        
        key, values = args[0], args[1:]
        
        entry = self.db.get(key)
        
        if not entry:
            self.db[key] = {
                "value": deque([]),
                "type": "list",
                "blocking_clients": deque([])
            }
            entry = self.db[key]
        
        if not self.ensure_list_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        entry["value"].extendleft(values)
        return RESPSerializer.serialize_integer(len(entry["value"]))


class LRangeCommand(RedisListCommandBase):
    """Implementation of LRANGE command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 3
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'lrange' command"
            )
        
        key = args[0]
        
        try: 
            start, stop = int(args[1]), int(args[2])
        except ValueError:
            return RESPSerializer.serialize_error(
                "ERR value is not an integer or out of range"
            )
        
        entry = self.db.get(key)
        
        if not entry:
            return RESPSerializer.serialize_array([])
        
        if not self.ensure_list_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        return RESPSerializer.serialize_array(
            # -1+1 = 0 -> 0 or None -> None(last element)
            list(entry["value"])[start:stop+1 or None] 
        )


class LLenCommand(RedisListCommandBase):
    """Implementation of LLEN command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 1
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'llen' command"
            )
        
        key = args[0]
        
        entry = self.db.get(key)
        
        if not entry:
            return RESPSerializer.serialize_integer(0)
        
        if not self.ensure_list_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        return RESPSerializer.serialize_integer(len(entry["value"]))


class LPopCommand(RedisListCommandBase):
    """Implementation of LPOP command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 1 or len(args) == 2
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'lpop' command"
            )
        
        key = args[0]
        
        try:
            num = int(args[1]) if len(args) == 2 else 1
        except ValueError:
            return RESPSerializer.serialize_error(
                "ERR value is not an integer or out of range"
            )
        
        entry = self.db.get(key)
        
        if not entry:
            return RESPSerializer.serialize_bulk_string(None)
        
        if not self.ensure_list_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        num = min(num, len(entry["value"]))
        popped_vals = [entry["value"].popleft() for _ in range(num)]
        
        self.cleanup_empty_key(key, entry)
        
        return (
            RESPSerializer.serialize_bulk_string(popped_vals[0])
            if num == 1
            else RESPSerializer.serialize_array(popped_vals)
        )


class BLPopCommand(RedisListCommandBase):
    """Implementation of BLPOP command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 2
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'blpop' command"
            )
        
        key = args[0]
        
        try:
            timeout = int(args[1])
        except ValueError:
            return RESPSerializer.serialize_error(
                "ERR value is not an integer or out of range"
            )
        
        entry = self.db.get(key)
        
        if not entry:
            self.db[key] = {
                "value": deque([]),
                "type": "list",
                "blocking_clients": deque([])
            }
            entry = self.db[key]
        
        if entry["value"]:
            return RESPSerializer.serialize_array(
                [key, entry["value"].popleft()]
            )
        else:
            waiter = {
                "event": Event(),
                "value": None
            }
            
            entry["blocking_clients"].append(waiter)
            
            signaled = waiter["event"].wait(None if timeout == 0 else timeout)
            if not signaled:
                entry["blocking_clients"].remove(waiter)
                return RESPSerializer.serialize_bulk_string(None)
            else:
                return RESPSerializer.serialize_array(
                    [key, waiter["value"]]
                )