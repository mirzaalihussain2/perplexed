import subprocess
import os
from typing import List
from pathlib import Path

def split_video(
    video_path: str,
    output_dir: str,
    clip_duration: int = 20
) -> List[str]:
    """
    Split video into clips of specified duration.
    Returns list of output file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        video_path
    ]
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        total_duration = float(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Failed to get video duration: {e.stderr}")
    
    num_clips = int(total_duration / clip_duration) + (1 if total_duration % clip_duration > 0 else 0)
    output_paths = []
    
    for i in range(num_clips):
        start_time = i * clip_duration
        output_path = os.path.join(output_dir, f"clip_{i:04d}.mp4")
        
        cmd = [
            'ffmpeg',
            '-i', video_path,
            '-ss', str(start_time),
            '-t', str(clip_duration),
            '-c:v', 'libx264',  # Re-encode video instead of copy
            '-c:a', 'aac',       # Re-encode audio instead of copy
            '-y',
            output_path
        ]
        
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            output_paths.append(output_path)
        except subprocess.CalledProcessError as e:
            raise ValueError(f"Failed to split video at {start_time}s: {e.stderr.decode()}")
    
    return output_paths

def extract_audio(video_path: str, output_path: str) -> str:
    """
    Extract audio from video file.
    Returns path to output audio file.
    """
    cmd = [
        'ffmpeg',
        '-i', video_path,
        '-vn',
        '-acodec', 'libmp3lame',
        '-ab', '192k',
        '-y',
        output_path
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return output_path
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Failed to extract audio: {e.stderr.decode()}")

def create_video_from_image_and_audio(
    image_path: str,
    audio_path: str,
    output_path: str
) -> str:
    """
    Create video from static image and audio file.
    Video duration matches audio duration.
    Image is scaled to 1280x720 (16:9) to match standard video format.
    Returns path to output video file.
    """
    cmd = [
        'ffmpeg',
        '-loop', '1',
        '-i', image_path,
        '-i', audio_path,
        '-vf', 'scale=1280:720:force_original_aspect_ratio=increase,crop=1280:720',
        '-c:v', 'libx264',
        '-tune', 'stillimage',
        '-c:a', 'aac',
        '-b:a', '192k',
        '-pix_fmt', 'yuv420p',
        '-shortest',
        '-y',
        output_path
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return output_path
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Failed to create video from image and audio: {e.stderr.decode()}")

def concat_videos(video_paths: List[str], output_path: str) -> str:
    """
    Concatenate multiple videos into one.
    Returns path to output video file.
    """
    concat_file = output_path + '.txt'
    
    with open(concat_file, 'w') as f:
        for path in video_paths:
            f.write(f"file '{os.path.abspath(path)}'\n")
    
    cmd = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', concat_file,
        '-c', 'copy',
        '-y',
        output_path
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return output_path
    finally:
        if os.path.exists(concat_file):
            os.remove(concat_file)