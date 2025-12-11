#!/usr/bin/env python3
"""
Simple YouTube -> MP3 downloader.

Usage: run `python download_mp3.py`, paste one or more YouTube URLs (comma or newline separated), and the script will download MP3 files.

Requirements: `yt-dlp` Python package and `ffmpeg` available in PATH.
"""

import os
import sys
import shutil
import re

try:
    import yt_dlp as youtube_dl
except Exception:
    print("Error: the Python package 'yt-dlp' is not installed. Run: python -m pip install -r requirements.txt")
    sys.exit(1)


def has_ffmpeg() -> bool:
    """Check if ffmpeg is available in PATH."""
    return shutil.which("ffmpeg") is not None


def sanitize_filename(name: str) -> str:
    """Remove characters that are invalid in file names on Windows and other systems."""
    # replace path separators and other odd characters
    name = re.sub(r"[\\/:*?\"<>|]+", "-", name)
    name = name.strip()
    return name


def download_mp3(url: str, quality: int = 192, outtmpl: str | None = None):
    """Download a single YouTube URL and convert to MP3.

    outtmpl: yt-dlp output template, default will use video title.
    quality: preferred mp3 bitrate (as string passed to yt-dlp postprocessor).
    """
    if not has_ffmpeg():
        print("ffmpeg not found in PATH. Please install ffmpeg and ensure it's available in your PATH.")
        return

    opts = {
        "format": "bestaudio/best",
        "outtmpl": outtmpl or "%(title)s.%(ext)s",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": str(quality),
            }
        ],
        # keep some info on-screen
        "quiet": False,
        "no_warnings": True,
    }

    try:
        with youtube_dl.YoutubeDL(opts) as ydl:
            print(f"Downloading and converting: {url}")
            ydl.download([url])
    except Exception as e:
        print(f"Failed to download {url}: {e}")


def main():
    print("YouTube -> MP3 downloader")
    print("Paste one or more YouTube URLs. You can separate multiple URLs with commas or newlines.")
    try:
        pasted = []
        # read multiple lines until EOF or blank line
        print("Enter URLs (press Enter twice to finish):")
        while True:
            try:
                line = input().strip()
            except EOFError:
                break
            if line == "":
                break
            pasted.append(line)

        if not pasted:
            print("No URL provided. Exiting.")
            return

        combined = "\n".join(pasted)
        # split by comma or newline and strip
        urls = [u.strip() for u in re.split(r"[\n,]+", combined) if u.strip()]

        for url in urls:
            download_mp3(url)

        print("Done.")
    except KeyboardInterrupt:
        print("Interrupted by user.")


if __name__ == "__main__":
    main()
