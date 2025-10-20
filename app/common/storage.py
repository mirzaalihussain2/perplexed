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

def upload_to_supabase(supabase_client, bucket: str, local_path: str, remote_path: str, content_type: str = None):
    """
    Upload file to Supabase storage.
    """
    import mimetypes
    
    with open(local_path, 'rb') as f:
        file_data = f.read()
    
    # Auto-detect content-type if not provided
    if content_type is None:
        content_type, _ = mimetypes.guess_type(local_path)
        if content_type is None:
            content_type = 'application/octet-stream'
    
    supabase_client.storage.from_(bucket).upload(
        remote_path,
        file_data,
        file_options={"content-type": content_type}
    )

def get_public_url(
    supabase: Client,
    bucket: str,
    remote_path: str
) -> str:
    """Get public URL for a file in Supabase storage"""
    return supabase.storage.from_(bucket).get_public_url(remote_path)