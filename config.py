import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration loader with defaults."""
    
    # LightRag
    @staticmethod
    def get_lightrag_url() -> str:
        return os.getenv("LIGHTRAG_URL", "http://localhost:9621")
    
    @staticmethod
    def get_api_key() -> Optional[str]:
        return os.getenv("LIGHTRAG_API_KEY")
    
    # Ingestion
    @staticmethod
    def get_root_dir() -> str:
        root = os.getenv("MARKDOWN_ROOT_DIR")
        if not root:
            raise ValueError("MARKDOWN_ROOT_DIR environment variable is not set")
        return root
    
    @staticmethod
    def get_concurrency() -> int:
        try:
            return int(os.getenv("CONCURRENCY", "8"))
        except ValueError:
            return 8
    
    @staticmethod
    def get_language() -> str:
        return os.getenv("LANGUAGE", "en")
    
    @staticmethod
    def get_batch_size() -> int:
        try:
            return int(os.getenv("QUERY_BATCH_SIZE", "1000"))
        except ValueError:
            return 1000
    
    # Process
    @staticmethod
    def get_log_level() -> str:
        return os.getenv("LOG_LEVEL", "INFO")
    
    @staticmethod
    def get_log_file() -> Path:
        return Path(os.getenv("LOG_FILE", "logs/ingestion.log"))
    
    @staticmethod
    def get_pid_file() -> Path:
        return Path(os.getenv("PID_FILE", "tmp/ingestion.pid"))
    
    @staticmethod
    def get_progress_file() -> Path:
        return Path(os.getenv("PROGRESS_FILE", "tmp/ingestion_progress.json"))
    
    # Features
    @staticmethod
    def get_force_reingest() -> bool:
        return os.getenv("FORCE_REINGEST", "false").lower() == "true"
    
    @staticmethod
    def get_skip_check() -> bool:
        return os.getenv("SKIP_CHECK", "false").lower() == "true"
    
    @staticmethod
    def get_max_retries() -> int:
        try:
            return int(os.getenv("MAX_RETRIES", "3"))
        except ValueError:
            return 3
    
    @staticmethod
    def get_retry_delay() -> int:
        try:
            return int(os.getenv("RETRY_DELAY", "5"))
        except ValueError:
            return 5
    
    @classmethod
    def validate(cls) -> bool:
        """Validate required configuration."""
        try:
            cls.get_root_dir()
            return True
        except ValueError:
            return False