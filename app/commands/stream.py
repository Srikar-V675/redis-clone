from .base import RedisCommand
from typing import List, Dict, Optional, Tuple
from collections import OrderedDict
from app.parser import RESPSerializer
import time
from threading import Event
import uuid

class RedisStreamCommandBase(RedisCommand):
    """Base class with common functionality for stream commands"""
    
    def __init__(self, db: Dict, waiting_clients: Dict):
        self.db = db
        self.waiting_clients = waiting_clients
    
    def ensure_stream_type(self, entry: Dict) -> bool:
        """Ensure whether accessed key is stream type"""
        return entry["type"] == "stream"
    
    def parse_id(self, id: str, is_start: Optional[bool] = None) -> Tuple:
        """Parses an ID into a tuple in the format (ms, seq) used by XRANGE and XREAD commands"""
        if id == "-":
            return (float("-inf"), float("-inf"))
        
        if id == "+":
            return (float("inf"), float("inf"))
        
        parts = id.split("-")
        if len(parts) == 1:
            return (int(parts[0]), 0) if is_start else (int(parts[0]), float("inf"))
        
        return (int(parts[0]), int(parts[1]))
    

class XAddCommand(RedisStreamCommandBase):
    """Implementation of XADD command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) >= 4 and (len(args) - 2) % 2 == 0 # stream_key ID key value... + missing pairs
    
    def validate_ID(self, last_id: Optional[str], id: str) -> Optional[bytes]:
        def error(msg:str) -> bytes:
            return RESPSerializer.serialize_error(msg)
        
        if not last_id and id == "0-0":
            return error("ERR The ID specified in XADD must be greater than 0-0")
        
        ms, seq = map(int, id.split("-"))
        
        if last_id:
            last_ms, last_seq = map(int, last_id.split("-"))
            
            if id == last_id:
                return error("ERR The ID specified in XADD is equal or smaller than the target stream top item")
            
            if ms < last_ms or (ms == last_ms and seq <= last_seq):
                return error("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        
        return None
    
    def generate_id(self, last_id: Optional[str], id: str) -> str:
        ms, seq = id.split("-")

        # Case 1: full auto (*-*)
        if ms == "*" and seq == "*":
            unix_time_ms = int(time.time() * 1000)
            # optional: check if last_id has same ms -> bump seq
            return f"{unix_time_ms}-0"

        # Case 2: partially specified (<ms>-*)
        if seq == "*":
            default_seq = 1 if ms == "0" else 0

            if last_id:
                last_ms, last_seq = last_id.split("-")
                if last_ms == ms:
                    return f"{ms}-{int(last_seq) + 1}"

            return f"{ms}-{default_seq}"

        # Case 3: explicit ID
        return id
            
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'xadd' command"
            )
        
        key, id, pairs = args[0], args[1], args[2:]
        
        entry = self.db.get(key)
        
        if not entry:
            self.db[key] = {
                "value": OrderedDict(),
                "type": "stream",
                "last_id": None
            }
            entry = self.db[key]
        
        if not self.ensure_stream_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        generated_id = self.generate_id(last_id = entry["last_id"], id = id)
        error = self.validate_ID(last_id = entry["last_id"], id = generated_id)
        if error:
            return error
                
        fields = {
            pairs[i]: pairs[i + 1]
            for i in range(0, len(pairs), 2)
        }
        entry["value"][generated_id] = fields
        entry["last_id"] = generated_id
        
        generated_tuple = self.parse_id(generated_id)
        for _, values in self.waiting_clients.items():
            if key in values["streams"] and generated_tuple >= values["streams"][key]:
                values["event"].set()
        
        return RESPSerializer.serialize_bulk_string(generated_id)


class XRangeCommand(RedisStreamCommandBase):
    """Implementation of XRANGE command"""
    
    def validate_args(self, args: List[str]) -> bool:
        return len(args) == 3
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'xrange' command"
            )
        
        key, start, stop = args
        
        entry = self.db.get(key)
        
        if not entry:
            return RESPSerializer.serialize_array([])
        
        if not self.ensure_stream_type(entry):
            return RESPSerializer.serialize_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        
        results = []
        start_tuple, stop_tuple = self.parse_id(start, is_start=True), self.parse_id(stop, is_start=False)
        
        for entry_id, field_values in entry["value"].items():
            entry_tuple = self.parse_id(entry_id)
            
            if entry_tuple < start_tuple: 
                continue
            elif entry_tuple > stop_tuple:
                break
            
            flat_fields = []
            for f, v in field_values.items():
                flat_fields.extend([f, v])
            
            results.append(
                [entry_id, flat_fields]
            )
        
        return RESPSerializer.serialize_array(results)


class XreadStreamsCommand(RedisStreamCommandBase):
    """Implementation of XREAD STREAMS command (non-blocking)"""
    
    def validate_args(self, args: List[str]) -> bool:
        # Must have at least "STREAMS" + one stream + one ID
        return len(args) >= 3 and (len(args) % 2) == 1
    
    def get_stream_entries(self, entry: Dict, start_id: str) -> List:
        """Return list of [entry_id, flat_fields] for entries >= start_id"""
        start_tuple = self.parse_id(start_id)
        results = []

        for entry_id, field_values in entry["value"].items():
            if self.parse_id(entry_id) >= start_tuple:
                flat_fields = [item for pair in field_values.items() for item in pair]
                results.append([entry_id, flat_fields])
        
        return results
    
    def get_multi_stream_results(self, stream_keys: List[str], start_ids: List[str]) -> List[List]:
        stream_results = []
        for stream_key, start_id in zip(stream_keys, start_ids):
            entry = self.db.get(stream_key)

            if entry and not self.ensure_stream_type(entry):
                raise TypeError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )

            entries = self.get_stream_entries(entry, start_id) if entry else []
            stream_results.append([stream_key, entries])
        
        return stream_results
    
    def execute(self, args: List[str]) -> bytes:
        if not self.validate_args(args):
            return RESPSerializer.serialize_error(
                "ERR wrong number of arguments for 'xread streams' command"
            )

        is_block = False
        
        if args[0].upper() == "BLOCK":
            is_block = True
            timeout = int(args[1])
            streams_and_ids = args[3:]
        else:
            _, *streams_and_ids = args
        
        mid = len(streams_and_ids) // 2
        stream_keys = streams_and_ids[:mid]
        start_ids = streams_and_ids[mid:]

        try:
            stream_results = self.get_multi_stream_results(stream_keys, start_ids)
            if is_block is False or any(len(entries) > 0 for _, entries in stream_results):
                return RESPSerializer.serialize_array(stream_results)
        except TypeError as e:
            return RESPSerializer.serialize_error(str(e))
        
        # `$` id not implemented yet
        waiter = {
            "event": Event(), 
            "streams": {
                k: self.parse_id(sid) 
                for k, sid in zip(stream_keys, start_ids)
            }
        }
        client_id = str(uuid.uuid4())
        self.waiting_clients[client_id] = waiter
        
        timeout_sec = None if timeout == 0 else timeout / 1000  # assuming ms -> sec
        waiter["event"].wait(timeout_sec)
        del self.waiting_clients[client_id]
        
        try:
            stream_results = self.get_multi_stream_results(stream_keys, start_ids)
            return RESPSerializer.serialize_array(stream_results)
        except TypeError as e:
            return RESPSerializer.serialize_error(str(e))
