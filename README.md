# YouTube -> MP3 Downloader

A small Python script that uses `yt-dlp` + `ffmpeg` to download the audio of a YouTube video and convert it to MP3.

Files:
- `download_mp3.py`: the downloader script (run with `python download_mp3.py`).
- `requirements.txt`: Python dependencies (`yt-dlp`).

Prerequisites
- Python 3.8+ installed.
- `ffmpeg` installed and available on your PATH.
  - On Windows you can install via Chocolatey (if you have it):

```powershell
choco install ffmpeg -y
```

  - Or download a static build from https://ffmpeg.org/download.html and add the `bin` folder to your PATH.

Install Python dependencies

```powershell
python -m pip install -r requirements.txt
```

Run

```powershell
python download_mp3.py
```

Paste one or more YouTube URLs (you can separate by commas or newlines). The script downloads audio and converts to MP3 using `ffmpeg`.

GUI version

There is a simple Tkinter GUI included: `youtube_downloader_gui.py`.

Run the GUI:

```powershell
python "c:\Users\Shou\Coding\Microsoft Visual Code\HUIT-Python-3\Youtube to Music\youtube_downloader_gui.py"
```

Features:
- Add single or multiple YouTube URLs
- Choose MP3 (audio) or MP4 (video)
- Set MP3 bitrate
- Select output directory and open it from the app
- Queue and start/stop downloads with progress and a log

Dependencies for GUI are the same: `yt-dlp` and `ffmpeg` on PATH. Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

Notes
- The script uses `yt-dlp`'s `FFmpegExtractAudio` postprocessor so `ffmpeg` must be available.
- Output filenames default to the YouTube video title. If you need more customization, edit `download_mp3.py`.

Security
- Only paste URLs you trust. The script will download content from the internet.

Enjoy!