from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = REPO_ROOT / "logs"


def ensure_logs_dir() -> Path:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR


def ensure_date_folder(date_str: str) -> Path:
    """Create and return date-specific log folder path"""
    date_folder = LOGS_DIR / date_str
    date_folder.mkdir(parents=True, exist_ok=True)
    return date_folder


def build_log_path(name: str, instrument: Optional[str] = None, when: Optional[datetime] = None) -> Path:
    """
    Create log file path with YYYYMMDD folder structure.
    Format: logs/YYYYMMDD/service_name[_instrument].log
    """
    stamp = when or datetime.now()
    date_folder_str = stamp.strftime("%Y%m%d")  # YYYYMMDD format
    safe_name = name.lower().replace(" ", "_")
    safe_instrument = f"_{instrument.lower()}" if instrument else ""
    
    # Create date folder and return file path
    date_folder = ensure_date_folder(date_folder_str)
    return date_folder / f"{safe_name}{safe_instrument}.log"


def build_instrument_log_path(service_name: str, instrument: str, when: Optional[datetime] = None) -> Path:
    """
    Create instrument-specific log file path with YYYYMMDD folder structure.
    Format: logs/YYYYMMDD/service_name_instrument.log

    """
    stamp = when or datetime.now()
    date_folder_str = stamp.strftime("%Y%m%d")  # YYYYMMDD format
    safe_service = service_name.lower().replace(" ", "_")
    safe_instrument = instrument.lower().replace(" ", "_")
    
    # Create date folder and return file path
    date_folder = ensure_date_folder(date_folder_str)
    return date_folder / f"{safe_service}_{safe_instrument}.log"


def cleanup_old_logs(retention_days: int = 7) -> int:
    """Clean up old log folders and files"""
    ensure_logs_dir()
    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = 0
    
    # Clean up date folders
    for date_folder in LOGS_DIR.iterdir():
        if not date_folder.is_dir() or not date_folder.name.isdigit():
            continue
            
        try:
            # Check folder modification time
            if datetime.fromtimestamp(date_folder.stat().st_mtime) < cutoff:
                # Delete all files in the folder
                for log_file in date_folder.glob("*.log"):
                    log_file.unlink(missing_ok=True)
                    deleted += 1
                # Delete the folder itself
                date_folder.rmdir()
                print(f"Deleted old log folder: {date_folder.name}")
        except Exception as e:
            print(f"Error cleaning up folder {date_folder}: {e}")
            continue
    
    # Also clean up old log files in root (for backward compatibility)
    for path in LOGS_DIR.glob("*.log"):
        try:
            if datetime.fromtimestamp(path.stat().st_mtime) < cutoff:
                path.unlink(missing_ok=True)
                deleted += 1
        except Exception:
            continue
    
    return deleted
