from supabase import Client
import os
import tempfile
from typing import Literal

async def download_from_supabase(
    supabase: Client,
    bucket: str,
    remote_path: str,
    local_path: str = None
) -> str:
    """
    Download file from Supabase storage to local filesystem
    Returns: local file path
    """
    try:
        response = supabase.storage.from_(bucket).download(remote_path)
        
        if local_path is None:
            _, ext = os.path.splitext(remote_path)
            fd, local_path = tempfile.mkstemp(suffix=ext)
            os.close(fd)
        
        with open(local_path, 'wb') as f:
            f.write(response)
        
        return local_path
    except Exception as error:
        raise ValueError(f"Failed to download {remote_path} from {bucket}: {error}")

def upload_to_supabase(
    supabase: Client,
    bucket: str,
    local_path: str,
    remote_path: str,
    content_type: str = "video/mp4"
) -> str:
    """
    Upload local file to Supabase storage
    Returns: remote path
    """
    try:
        with open(local_path, "rb") as f:
            response = supabase.storage.from_(bucket).upload(
                file=f,
                path=remote_path,
                file_options={
                    "upsert": "true",
                    "content-type": content_type
                }
            )
        return response.path
    except Exception as error:
        raise ValueError(f"Failed to upload {local_path} to {bucket}/{remote_path}: {error}")

def get_public_url(
    supabase: Client,
    bucket: str,
    remote_path: str
) -> str:
    """Get public URL for a file in Supabase storage"""
    return supabase.storage.from_(bucket).get_public_url(remote_path)