from typing import List, Optional, Tuple

class RESPSerializer:
    """
    Handles Redis RESP(Redis Serialization Protocol) serialization
    """
    
    @staticmethod
    def serialize_simple_string(message: str) -> bytes:
        """
        Serialize a simple string: `+data\r\n`

        Args:
            message (str)

        Returns:
            bytes
        """
        return f"+{message}\r\n".encode("utf-8")
    
    @staticmethod
    def serialize_error(message: str) -> bytes:
        """Serialize a error: -<data>\r\n

        Args:
            message (str)

        Returns:
            bytes
        """
        return f"-{message}\r\n".encode("utf-8")
    
    @staticmethod
    def serialize_integer(value: int) -> bytes:
        """Serialize an integer: :<value>\r\n

        Args:
            value (int)

        Returns:
            bytes
        """
        return f":{value}\r\n".encode("utf-8")
    
    @staticmethod
    def serialize_bulk_string(data: Optional[str]) -> bytes:
        """Serialize a bulk string: $<length>\r\n<data>\r\n or _\r\n for null

        Args:
            data (Optional[str])

        Returns:
            bytes
        """
        if not data:
            return b"_\r\n"
        return f"${len(data)}\r\n{data}\r\n".encode("utf-8")
    
    @staticmethod
    def serialize_array(items: List[str, None]) -> bytes:
        """Serialize an array: *<num-of-elements>\r\n<element-1>\r\n...

        Args:
            items (List[str])

        Returns:
            bytes
        """
        if not items:
            return b"*0\r\n"
        
        result = f"*{len(items)}\r\n".encode("utf-8")
        for item in items:
            if item is None:
                result += b"_\r\n"
            else:
                result += RESPSerializer.serialize_bulk_string(item)
        return result


class RESPParser:
    """Handles Redis RESP(Redis Serialization Protocol) parsing
    """
    
    @staticmethod
    def parse_request(data: bytes) -> Optional[List[str]]:
        """
        Parse a Redis command from bytes -> command and list of strings. 
        Handles RESP array format: *<number-of-elements>\r\n<element-1>\r\n<element-2>\r\n...

        Args:
            data (bytes)

        Returns:
            Optional[Tuple[str, List[str]]]
        """
        try:
            lines = data.decode().split("\r\n")
            if not lines or not lines[0].startswith("*"):
                return None
            
            num_elements = int(data[0][1:])
            if num_elements <= 0:
                return []
            
            tokens = []
            
            i = 1
            while i < len(lines) and len(tokens) < num_elements:
                # bulk string
                if lines[i].startswith("$"):
                    length = int(lines[i][1:])
                    i += 1
                    if length >= 0:
                        tokens.append(lines[i])
                    else:
                        tokens.append("")
                    i += 1
                # simple string 
                else:
                    tokens.append(lines[i])
                    i += 1
            
            return tokens
        
        except (ValueError, IndexError, UnicodeDecodeError) as e:
            print(f"Error parsing command: {e}")
            return None