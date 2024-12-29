import tarfile
import os
import shutil
from pathlib import Path

def compress_directory(dir_path: str, output_path: str) -> bool:
    """
    Compresses a directory into a tar.gz archive.
    
    Args:
        dir_path: Path to the directory to compress
        output_path: Path where the compressed archive should be saved
        
    Returns:
        bool: True if compression was successful, False otherwise
        
    Raises:
        FileNotFoundError: If the source directory doesn't exist
        PermissionError: If there are permission issues
    """
    try:
        dir_path = Path(dir_path)
        output_path = Path(output_path)
        
        if not dir_path.is_dir():
            raise FileNotFoundError(f"Directory not found: {dir_path}")
            
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with tarfile.open(output_path, "w:gz") as tar:
            tar.add(dir_path, arcname=dir_path.name)
            
        return True
    except (OSError, tarfile.TarError) as e:
        print(f"Error compressing directory: {e}")
        return False

def decompress_archive(archive_path: str, output_path: str) -> bool:
    """
    Decompresses a tar.gz archive.
    
    Args:
        archive_path: Path to the archive to decompress
        output_path: Path where the contents should be extracted
        
    Returns:
        bool: True if decompression was successful, False otherwise
        
    Raises:
        FileNotFoundError: If the archive doesn't exist
        PermissionError: If there are permission issues
    """
    try:
        archive_path = Path(archive_path)
        output_path_Path = Path(output_path)
        
        if not archive_path.is_file():
            raise FileNotFoundError(f"Archive not found: {archive_path}")
            
        # Ensure output directory exists
        output_path_Path.mkdir(parents=True, exist_ok=True)
        
        with tarfile.open(archive_path, "r:gz") as tar:
            # Check for any suspicious paths before extracting
            for member in tar.getmembers():
                if member.name.startswith('/') or '..' in member.name:
                    raise ValueError(f"Suspicious path in archive: {member.name}")
            # Extract all files
            tar.extractall(path=output_path_Path)
            
        return os.path.join(output_path,archive_path.name.rstrip('.tar.gz'))
    except (OSError, tarfile.TarError, ValueError) as e:
        print(f"Error decompressing archive: {e}")
        return False

def ensure_directory(path: str) -> bool:
    """
    Ensures a directory exists, creating it if necessary.
    
    Args:
        path: Path to the directory
        
    Returns:
        bool: True if directory exists or was created successfully
        
    Raises:
        PermissionError: If there are permission issues
    """
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        return True
    except OSError as e:
        print(f"Error ensuring directory exists: {e}")
        return False

def clean_directory(path: str) -> bool:
    """
    Removes all contents of a directory without removing the directory itself.
    
    Args:
        path: Path to the directory to clean
        
    Returns:
        bool: True if cleaning was successful
        
    Raises:
        FileNotFoundError: If the directory doesn't exist
        PermissionError: If there are permission issues
    """
    try:
        path = Path(path)
        if not path.is_dir():
            raise FileNotFoundError(f"Directory not found: {path}")
            
        for item in path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
                
        return True
    except OSError as e:
        print(f"Error cleaning directory: {e}")
        return False
