import pytest
import os
from pathlib import Path
from app.common.video import (
    split_video,
    extract_audio,
    create_video_from_image_and_audio,
    concat_videos
)

TEST_VIDEO = "test-files/ferriss.mp4"

def test_split_video(tmp_path):
    """Test splitting video into clips"""
    output_dir = str(tmp_path / "clips")
    
    clips = split_video(TEST_VIDEO, output_dir, clip_duration=10)
    
    assert len(clips) > 0
    for clip in clips:
        assert os.path.exists(clip)
        assert os.path.getsize(clip) > 0

def test_extract_audio(tmp_path):
    """Test extracting audio from video"""
    output_dir = str(tmp_path / "clips")
    clips = split_video(TEST_VIDEO, output_dir, clip_duration=10)
    
    audio_path = str(tmp_path / "audio.mp3")
    result = extract_audio(clips[0], audio_path)
    
    assert result == audio_path
    assert os.path.exists(audio_path)
    assert os.path.getsize(audio_path) > 0

def test_create_video_from_image_and_audio(tmp_path):
    """Test creating video from image and audio"""
    output_dir = str(tmp_path / "clips")
    clips = split_video(TEST_VIDEO, output_dir, clip_duration=5)
    
    audio_path = str(tmp_path / "audio.mp3")
    extract_audio(clips[0], audio_path)
    
    image_path = str(tmp_path / "test_image.png")
    os.system(f"ffmpeg -i {clips[0]} -vframes 1 -y {image_path} 2>/dev/null")
    
    output_video = str(tmp_path / "output.mp4")
    result = create_video_from_image_and_audio(image_path, audio_path, output_video)
    
    assert result == output_video
    assert os.path.exists(output_video)
    assert os.path.getsize(output_video) > 0

def test_concat_videos(tmp_path):
    """Test concatenating multiple videos"""
    output_dir = str(tmp_path / "clips")
    clips = split_video(TEST_VIDEO, output_dir, clip_duration=10)
    
    first_three = clips[:3]
    
    output_video = str(tmp_path / "concatenated.mp4")
    result = concat_videos(first_three, output_video)
    
    assert result == output_video
    assert os.path.exists(output_video)
    assert os.path.getsize(output_video) > 0

def test_full_pipeline(tmp_path):
    """Test complete workflow: split -> extract audio -> create replacement -> concat"""
    clips_dir = str(tmp_path / "clips")
    clips = split_video(TEST_VIDEO, clips_dir, clip_duration=5)
    
    assert len(clips) >= 2
    
    audio_path = str(tmp_path / "audio.mp3")
    extract_audio(clips[0], audio_path)
    
    image_path = str(tmp_path / "frame.png")
    os.system(f"ffmpeg -i {clips[0]} -vframes 1 -y {image_path} 2>/dev/null")
    
    replacement_video = str(tmp_path / "replacement.mp4")
    create_video_from_image_and_audio(image_path, audio_path, replacement_video)
    
    final_clips = [replacement_video] + clips[1:]
    
    final_video = str(tmp_path / "final.mp4")
    concat_videos(final_clips, final_video)
    
    assert os.path.exists(final_video)
    assert os.path.getsize(final_video) > 0