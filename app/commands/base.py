from abc import ABC, abstractmethod
from typing import List

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