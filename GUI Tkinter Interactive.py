#!/usr/bin/env python3
"""
YouTube Downloader GUI (Tkinter)

Features:
- Add one or more YouTube URLs to a queue
- Choose output format: MP3 or MP4
- Choose MP3 bitrate
- Select output directory
- Start / Stop downloads
- Shows per-item progress and a log area

Dependencies: `yt-dlp`, `ffmpeg` on PATH

Run:
    python youtube_downloader_gui.py

"""
import os
import sys
import threading
import queue
import time
import concurrent.futures
from pathlib import Path
import shutil
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    print("This script requires a GUI environment with tkinter available.")
    raise

try:
    import yt_dlp as youtube_dl
except Exception:
    youtube_dl = None

try:
    from PIL import Image, ImageTk
except Exception:
    Image = None
    ImageTk = None
 
try:
    import vlc
    has_vlc = True
except Exception:
    vlc = None
    has_vlc = False


class DownloadItem:
    def __init__(self, url, fmt='mp3', bitrate='192'):
        self.url = url
        self.title = None
        self.fmt = fmt
        self.bitrate = bitrate
        self.status = 'Queued'
        self.progress = 0.0
        self.filename = None
        self.error = None


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        # Try to pick a nicer theme if available
        try:
            style = ttk.Style()
            if 'vista' in style.theme_names():
                style.theme_use('vista')
            elif 'clam' in style.theme_names():
                style.theme_use('clam')
        except Exception:
            pass
        self.title('YouTube Downloader — MP3 / MP4')
        # professional default size and sensible minimums
        self.geometry('1100x700')
        self.minsize(900, 600)

        self.queue_items = []  # list[DownloadItem]
        self.work_thread = None
        self.stop_requested = threading.Event()
        self.event_q = queue.Queue()
        # Thread pool for background tasks (thumbnails/search parallelism)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        self.search_cache = {}
        # locate ffmpeg if available
        self.ffmpeg_path = self._find_ffmpeg()

        self._build_ui()
        self._schedule_poll()
        # ensure executor shuts down on exit
        self.protocol('WM_DELETE_WINDOW', self._on_app_close)

    def _build_ui(self):
        pad = 8
        # Menubar
        try:
            menubar = tk.Menu(self)
            file_menu = tk.Menu(menubar, tearoff=0)
            file_menu.add_command(label='Exit', accelerator='Ctrl+Q', command=self._on_app_close)
            menubar.add_cascade(label='File', menu=file_menu)
            help_menu = tk.Menu(menubar, tearoff=0)
            help_menu.add_command(label='About', command=lambda: messagebox.showinfo('About', 'YouTube Downloader — Tkinter GUI'))
            menubar.add_cascade(label='Help', menu=help_menu)
            self.config(menu=menubar)
        except Exception:
            pass

        # Toolbar
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=pad, pady=(pad, 0))

        self.url_var = tk.StringVar()
        url_entry = ttk.Entry(toolbar, textvariable=self.url_var)
        url_entry.pack(side='left', fill='x', expand=True, padx=(0,6))

        add_btn = ttk.Button(toolbar, text='➕ Add', command=self.on_add_url)
        add_btn.pack(side='left', padx=(0,4))
        paste_btn = ttk.Button(toolbar, text='📋 Paste', command=self.on_paste_add)
        paste_btn.pack(side='left', padx=(0,4))
        search_btn = ttk.Button(toolbar, text='🔎 Search', command=self.on_search)
        search_btn.pack(side='left', padx=(0,6))

        # Format and bitrate compact group
        self.format_var = tk.StringVar(value='mp3')
        fmt_frame = ttk.Frame(toolbar)
        fmt_frame.pack(side='left', padx=(0,8))
        ttk.Label(fmt_frame, text='Format:').pack(side='left', padx=(0,4))
        fmt_combo = ttk.Combobox(fmt_frame, textvariable=self.format_var, values=['mp3','mp4'], width=6, state='readonly')
        fmt_combo.pack(side='left')

        self.bitrate_var = tk.StringVar(value='192')
        self.quality_var = tk.StringVar()
        self.quality_combo = None
        self.bitrate_lbl = ttk.Label(fmt_frame, text='Bitrate:')
        self.bitrate_lbl.pack(side='left', padx=(8,4))
        self.bitrate_combo = ttk.Combobox(fmt_frame, textvariable=self.bitrate_var, values=['128','160','192','256','320'], width=6, state='readonly')
        self.bitrate_combo.pack(side='left')

        def update_quality_options(*args):
            fmt = self.format_var.get()
            # debug log for tracing why quality list may be empty
            try:
                self.event_q.put(('log', f'update_quality_options() called — fmt={fmt} url="{self.url_var.get().strip()}"'))
            except Exception:
                pass
            if fmt == 'mp4':
                self.bitrate_lbl.pack_forget()
                self.bitrate_combo.pack_forget()
                if not self.quality_combo:
                    self.quality_combo = ttk.Combobox(fmt_frame, textvariable=self.quality_var, state='readonly', width=22)
                    # bind selection change to apply quality to selected item or default
                    try:
                        self.quality_combo.bind('<<ComboboxSelected>>', lambda e: self._on_toolbar_quality_change())
                    except Exception:
                        pass
                # show a placeholder while fetching formats
                try:
                    self.quality_combo['values'] = ['Fetching...']
                    self.quality_combo.set('Fetching...')
                    self.quality_combo.pack(side='left', padx=(8,4))
                except Exception:
                    pass

                url = self.url_var.get().strip() or getattr(self, 'selected_url', '')
                # quick guard: ensure yt-dlp is available
                if youtube_dl is None:
                    try:
                        self.quality_combo['values'] = ['yt-dlp missing']
                        self.quality_combo.set('yt-dlp missing')
                        self.quality_combo.pack(side='left', padx=(8,4))
                        self.event_q.put(('log', 'yt-dlp not available in Python environment — cannot fetch formats'))
                    except Exception:
                        pass
                    return
                if not url:
                    # nothing to fetch yet - show No formats
                    try:
                        self.quality_combo['values'] = ['No formats']
                        self.quality_combo.set('No formats')
                        self.quality_format_ids = []
                        self.quality_combo.pack(side='left', padx=(8,4))
                    except Exception:
                        pass
                    return

                # fetch formats in background (centralized implementation)
                try:
                    self._fetch_formats_background(url)
                except Exception:
                    pass
            else:
                if self.quality_combo:
                    try:
                        self.quality_combo.pack_forget()
                    except Exception:
                        pass
                self.bitrate_lbl.pack(side='left', padx=(8,4))
                self.bitrate_combo.pack(side='left')
        self.format_var.trace_add('write', update_quality_options)

        # Main paned layout: left = queue, right = details/log
        paned = ttk.Panedwindow(self, orient='horizontal')
        paned.pack(fill='both', expand=True, padx=pad, pady=(6, pad))

        # Left pane: queue
        left = ttk.Frame(paned)
        paned.add(left, weight=3)

        cols = ('title', 'status', 'progress')
        self.tree = ttk.Treeview(left, columns=cols, show='headings', selectmode='browse')
        self.tree.heading('title', text='Title')
        self.tree.heading('status', text='Status')
        self.tree.heading('progress', text='Progress')
        self.tree.column('title', width=600, stretch=True)
        self.tree.column('status', width=110, anchor='center')
        self.tree.column('progress', width=90, anchor='center')
        self.tree.pack(fill='both', expand=True, side='left')
        self.tree.bind('<Double-Button-1>', self.on_open_output)
        self.tree.bind('<<TreeviewSelect>>', self._on_tree_select)

        q_scroll = ttk.Scrollbar(left, orient='vertical', command=self.tree.yview)
        q_scroll.pack(side='left', fill='y')
        # use a wrapper for yscrollcommand so we can redraw overlays when scrolling
        self._queue_scrollbar = q_scroll
        self.tree.config(yscrollcommand=self._on_tree_yscroll)

        # mapping iid -> item
        self.iid_map = {}

        # create overlay canvas for per-row graphical progress bars
        try:
            self._create_progress_overlay(parent=left)
        except Exception:
            pass

        # Right pane: details + log
        right = ttk.Frame(paned, width=380)
        paned.add(right, weight=2)

        # Output folder quick access
        out_frame = ttk.Frame(right)
        out_frame.pack(fill='x', padx=6, pady=(0,8))
        self.output_dir = tk.StringVar(value=str(Path.cwd()))
        out_entry = ttk.Entry(out_frame, textvariable=self.output_dir)
        out_entry.pack(side='left', fill='x', expand=True)
        choose_btn = ttk.Button(out_frame, text='Browse', command=self.choose_output_dir)
        choose_btn.pack(side='left', padx=(6,0))
        ff_btn = ttk.Button(out_frame, text='Set ffmpeg', command=self.choose_ffmpeg)
        ff_btn.pack(side='left', padx=(6,0))

        # Preview area
        preview = ttk.LabelFrame(right, text='Preview')
        preview.pack(fill='x', padx=6, pady=(0,8))
        self.thumb_label = ttk.Label(preview, text='No selection', anchor='center')
        self.thumb_label.config(width=40)
        self.thumb_label.pack(fill='both', expand=True, padx=6, pady=6)

        meta_frame = ttk.Frame(preview)
        meta_frame.pack(fill='x', padx=6, pady=(0,6))
        self.meta_title = tk.StringVar(value='Title: —')
        ttk.Label(meta_frame, textvariable=self.meta_title, wraplength=320).pack(anchor='w')

        btns = ttk.Frame(preview)
        btns.pack(fill='x', padx=6)
        self.play_btn = ttk.Button(btns, text='▶ Play', command=self.play_selected)
        self.play_btn.pack(side='left', fill='x', expand=True, padx=(0,6))
        open_out_btn = ttk.Button(btns, text='Open folder', command=self.open_output_folder)
        open_out_btn.pack(side='left', fill='x', expand=True)

        # Controls
        ctrl_frame = ttk.LabelFrame(right, text='Controls')
        ctrl_frame.pack(fill='x', padx=6, pady=(8,6))
        start_btn = ttk.Button(ctrl_frame, text='⬇ Start', command=self.start_downloads)
        start_btn.pack(fill='x', padx=6, pady=4)
        stop_btn = ttk.Button(ctrl_frame, text='■ Stop', command=self.stop_downloads)
        stop_btn.pack(fill='x', padx=6, pady=(0,4))
        remove_btn = ttk.Button(ctrl_frame, text='Remove Selected', command=self.remove_selected)
        remove_btn.pack(fill='x', padx=6, pady=(0,4))
        clear_btn = ttk.Button(ctrl_frame, text='Clear Queue', command=self.clear_queue)
        clear_btn.pack(fill='x', padx=6, pady=(0,4))

        # Log area occupies remaining right pane space
        log_frame = ttk.LabelFrame(right, text='Log')
        log_frame.pack(fill='both', expand=True, padx=6, pady=(0,6))
        self.log_text = tk.Text(log_frame, height=8, wrap='none')
        self.log_text.pack(fill='both', expand=True, side='left')
        log_scroll = ttk.Scrollbar(log_frame, orient='vertical', command=self.log_text.yview)
        log_scroll.pack(side='left', fill='y')
        self.log_text.config(yscrollcommand=log_scroll.set)

        # Status bar and overall progress
        footer = ttk.Frame(self)
        footer.pack(fill='x')
        self.status_var = tk.StringVar(value='Ready')
        status = ttk.Label(footer, textvariable=self.status_var, anchor='w')
        status.pack(side='left', fill='x', expand=True, padx=(6,0), pady=(0,6))
        self.overall_progress = ttk.Progressbar(footer, length=220, mode='determinate')
        self.overall_progress.pack(side='right', padx=6, pady=(4,6))

        # keyboard shortcuts
        self.bind_all('<Control-q>', lambda e: self._on_app_close())
        self.bind_all('<Control-Q>', lambda e: self._on_app_close())
        self.bind_all('<Control-s>', lambda e: self.on_search())
        self.bind_all('<Control-p>', lambda e: self.on_paste_add())

        # small visual style adjustments
        try:
            style = ttk.Style()
            style.configure('Treeview.Heading', font=('Segoe UI', 10, 'bold'))
            style.configure('TButton', padding=6)
            # compact tree font for better density
            style.configure('Treeview', font=('Segoe UI', 9))
        except Exception:
            pass

    def _on_app_close(self):
        # Graceful shutdown: stop worker, shutdown executor, then destroy window
        try:
            if messagebox.askokcancel('Quit', 'Are you sure you want to exit?'):
                try:
                    self.stop_requested.set()
                except Exception:
                    pass
                try:
                    # do not block long on executor shutdown
                    self.executor.shutdown(wait=False)
                except Exception:
                    pass
                try:
                    if self.work_thread and self.work_thread.is_alive():
                        # give worker a short time to stop
                        self.work_thread.join(timeout=1.0)
                except Exception:
                    pass
                try:
                    self.destroy()
                except Exception:
                    try:
                        sys.exit(0)
                    except Exception:
                        pass
        except Exception:
            # fallback: force destroy
            try:
                self.destroy()
            except Exception:
                pass

    def on_add_url(self):
        text = self.url_var.get().strip()
        if not text:
            return
        inputs = [u.strip() for u in text.replace(',', '\n').splitlines() if u.strip()]
        for inp in inputs:
            # If input looks like a URL, add and fetch title in background
            if inp.lower().startswith('http') or 'youtube.com' in inp or 'youtu.be' in inp:
                it = DownloadItem(inp, fmt=self.format_var.get(), bitrate=self.bitrate_var.get())
                it.status = 'Fetching title...'
                # if mp4 format and toolbar quality selected, attach quality format id to item
                try:
                    if it.fmt == 'mp4' and getattr(self, 'quality_combo', None):
                        sel = self.quality_combo.get()
                        if sel and sel != 'No formats' and getattr(self, 'quality_format_ids', None):
                            try:
                                idx = list(self.quality_combo['values']).index(sel)
                                it.quality = self.quality_format_ids[idx]
                                it.quality_label = sel
                            except Exception:
                                it.quality = None
                        else:
                            # fallback to previously stored default quality if available
                            it.quality = getattr(self, 'default_quality', None)
                            it.quality_label = getattr(self, 'default_quality_label', None)
                except Exception:
                    it.quality = None
                self.queue_items.append(it)
                idx = len(self.queue_items) - 1
                iid = str(id(it))
                self.iid_map[iid] = it
                # insert into tree with placeholder title
                self.tree.insert('', 'end', iid=iid, values=(it.url, it.status, self._format_progress_bar(it.progress)))
                # fetch title in background
                threading.Thread(target=self._fetch_title_and_update, args=(idx, it), daemon=True).start()
            else:
                # treat as search query on YouTube
                threading.Thread(target=self._search_and_add, args=(inp,), daemon=True).start()
        self.url_var.set('')
        try:
            self._draw_progress_overlays()
        except Exception:
            pass

    def on_paste_add(self):
        try:
            txt = self.clipboard_get()
        except Exception:
            txt = ''
        self.url_var.set(txt)
        self.on_add_url()

    def on_search(self):
        text = self.url_var.get().strip()
        if not text:
            return
        # launch search in background to keep UI responsive
        threading.Thread(target=self._search_and_add, args=(text, 20), daemon=True).start()

    def _on_tree_select(self, event=None):
        sel = self.tree.selection()
        if not sel:
            self.thumb_label.config(text='No selection')
            try:
                self.play_btn.state(['disabled'])
            except Exception:
                pass
            return
        iid = sel[0]
        item = self.iid_map.get(iid)
        if not item:
            self.thumb_label.config(text='No selection')
            try:
                self.play_btn.state(['disabled'])
            except Exception:
                pass
            return
        # show thumbnail if available
        # update metadata preview
        try:
            self.meta_title.set(f"Title: {item.title or item.url}")
        except Exception:
            pass

        if getattr(item, 'thumbnail_image', None):
            try:
                self.thumb_label.config(image=item.thumbnail_image)
                self.thumb_label.image = item.thumbnail_image
            except Exception:
                self.thumb_label.config(text=(item.title or item.url))
        else:
            # attempt to fetch thumbnail in background
            self.thumb_label.config(text='Loading thumbnail...')
            threading.Thread(target=self._fetch_thumbnail, args=(item,iid), daemon=True).start()
        # If URL is YouTube, fetch formats for toolbar quality options
        try:
            if 'youtube.com' in item.url or 'youtu.be' in item.url:
                self.selected_url = item.url  # store for quality fetch
                # fetch formats based on this item URL so toolbar shows matching qualities
                try:
                    self._fetch_formats_background(item.url)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self.play_btn.state(['!disabled'])
        except Exception:
            pass

    def _open_player_window(self, item: DownloadItem):
        # Embedded player window using python-vlc with full controls
        if not has_vlc:
            messagebox.showerror('Player missing', 'Embedded player requires python-vlc and libvlc installed. Please install VLC and the python-vlc package.')
            return
        # Resolve direct stream URL via yt-dlp (if available) to avoid VLC's youtube.lua descramble issues
        media_url = item.url
        if youtube_dl is not None:
            try:
                resolved = self._resolve_stream_url(item)
                if resolved:
                    media_url = resolved
            except Exception as e:
                # continue with original URL but warn
                self.log('Stream resolve failed:', e)

        class PlayerWindow(tk.Toplevel):
            def __init__(self, master, media_url, title=None):
                super().__init__(master)
                self.title(title or 'Player')
                self.geometry('900x560')
                self.protocol('WM_DELETE_WINDOW', self._on_close)
                # remember original media page/url for resolution attempts
                try:
                    self._original_media_url = media_url
                except Exception:
                    self._original_media_url = None

                # Disable libVLC hardware-accelerated decoding to avoid D3D11VA/DXVA failures
                # on some systems when playing very high-resolution streams (8K).
                try:
                    inst = self._create_vlc_instance()
                    if inst is not None:
                        self.instance = inst
                    else:
                        self.instance = vlc.Instance()
                except Exception:
                    try:
                        self.instance = vlc.Instance()
                    except Exception:
                        self.instance = None
                self.player = self.instance.media_player_new()
                # create media from resolved stream (or page URL). If instance is None, try default
                if self.instance is None:
                    try:
                        self.instance = self._create_vlc_instance() or (vlc.Instance() if vlc else None)
                    except Exception:
                        try:
                            self.instance = vlc.Instance()
                        except Exception:
                            self.instance = None
                if self.instance is None:
                    # last resort: do not set media via VLC
                    return
                # Guard: if media_url appears to be a thumbnail/storyboard image (ytimg),
                # fall back to the canonical YouTube page URL so VLC uses youtube.lua.
                try:
                    mus = (media_url or '').lower()
                    if mus.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in mus or 'storyboard' in mus or 'googleusercontent' in mus:
                        try:
                            self.master.event_q.put(('log', f'Player: media_url looks like image/storyboard, falling back to page URL: {media_url[:160]}'))
                        except Exception:
                            pass
                        # prefer original page URL when available
                        try:
                            media_url = getattr(item, 'url', media_url)
                        except Exception:
                            pass
                except Exception:
                    pass
                self.media = self.instance.media_new(media_url)
                # Defensive media options: disable hardware decoding and increase network caching
                try:
                    # libVLC media options use ':' prefix
                    self.media.add_option(':avcodec-hw=none')
                    self.media.add_option(':hwdec=0')
                    self.media.add_option(':no-video-title-show')
                    self.media.add_option(':network-caching=2000')
                    self.media.add_option(':direct3d11-hw-blending=no')
                    self.media.add_option(':avcodec-threads=1')
                except Exception:
                    pass
                self.player.set_media(self.media)

                # Video area
                self.video_frame = ttk.Frame(self)
                self.video_frame.pack(fill='both', expand=True)
                self.canvas = tk.Canvas(self.video_frame, bg='black')
                self.canvas.pack(fill='both', expand=True)

                # Controls
                ctrl = ttk.Frame(self)
                ctrl.pack(fill='x')

                self.play_state = tk.StringVar(value='Play')
                self.play_btn = ttk.Button(ctrl, textvariable=self.play_state, command=self.toggle_play)
                self.play_btn.pack(side='left', padx=6, pady=6)

                self.stop_btn = ttk.Button(ctrl, text='Stop', command=self.stop)
                self.stop_btn.pack(side='left', padx=6, pady=6)

                self.rew_btn = ttk.Button(ctrl, text='<< 10s', command=lambda: self.skip(-10))
                self.rew_btn.pack(side='left', padx=4)
                self.fwd_btn = ttk.Button(ctrl, text='10s >>', command=lambda: self.skip(10))
                self.fwd_btn.pack(side='left', padx=4)

                # time labels and slider
                self.time_lbl = ttk.Label(ctrl, text='00:00 / 00:00')
                self.time_lbl.pack(side='left', padx=8)

                self.pos_var = tk.DoubleVar()
                self.slider = ttk.Scale(ctrl, orient='horizontal', from_=0, to=1000, variable=self.pos_var, command=self.on_seek)
                self.slider.pack(fill='x', expand=True, side='left', padx=6)

                # volume
                vol_lbl = ttk.Label(ctrl, text='Vol')
                vol_lbl.pack(side='left', padx=4)
                self.vol_var = tk.DoubleVar(value=100)
                vol_slider = ttk.Scale(ctrl, orient='horizontal', from_=0, to=100, variable=self.vol_var, command=self.on_vol)
                vol_slider.pack(side='left', padx=6)

                self._updating = False
                self._is_playing = False

                # set window handle after small delay (longer to let the window initialize)
                try:
                    # bring window briefly to front so it doesn't appear behind other dialogs
                    try:
                        self.attributes('-topmost', True)
                        self.lift()
                        self.focus_force()
                        # remove topmost after small delay
                        self.after(300, lambda: self.attributes('-topmost', False))
                    except Exception:
                        pass
                except Exception:
                    pass
                self.after(600, self._set_hwnd)
                # start polling position
                self._job = self.after(500, self.update_position)

            def _set_hwnd(self):
                try:
                    hwnd = self.canvas.winfo_id()
                    if sys.platform.startswith('win'):
                        self.player.set_hwnd(hwnd)
                    elif sys.platform.startswith('linux'):
                        self.player.set_xwindow(hwnd)
                    elif sys.platform.startswith('darwin'):
                        self.player.set_nsobject(hwnd)
                except Exception:
                    pass

            def toggle_play(self):
                try:
                    if self.player.is_playing():
                        self.player.pause()
                        self.play_state.set('Play')
                        self._is_playing = False
                    else:
                        # play (start)
                        self.player.play()
                        self.player.audio_set_volume(int(self.vol_var.get()))
                        self.play_state.set('Pause')
                        self._is_playing = True
                except Exception as e:
                    self.master.log('Player error:', e)

            def stop(self):
                try:
                    self.player.stop()
                except Exception:
                    pass
                self.play_state.set('Play')
                self._is_playing = False

            def skip(self, seconds):
                try:
                    length = self.player.get_length() / 1000.0
                    cur = self.player.get_time() / 1000.0
                    target = max(0, min(length, cur + seconds))
                    self.player.set_time(int(target * 1000))
                except Exception:
                    pass

            def on_seek(self, val):
                if self._updating:
                    return
                try:
                    pos = float(val) / 1000.0
                    length = self.player.get_length()
                    if length > 0:
                        new_time = int(pos * length)
                        self.player.set_time(new_time)
                except Exception:
                    pass

            def on_vol(self, val):
                try:
                    self.player.audio_set_volume(int(float(val)))
                except Exception:
                    pass

            def update_position(self):
                try:
                    length = self.player.get_length()
                    if length > 0:
                        cur = self.player.get_time()
                        # update slider and time label
                        self._updating = True
                        try:
                            pos = cur / length
                            self.pos_var.set(pos * 1000)
                        finally:
                            self._updating = False
                        # format time
                        def fmt(ms):
                            s = int(ms/1000)
                            m, s = divmod(s, 60)
                            h, m = divmod(m, 60)
                            if h:
                                return f"{h:02d}:{m:02d}:{s:02d}"
                            return f"{m:02d}:{s:02d}"

                        self.time_lbl.config(text=f"{fmt(cur)} / {fmt(length)}")
                except Exception:
                    pass
                self._job = self.after(500, self.update_position)

            def _on_close(self):
                try:
                    if self._job:
                        self.after_cancel(self._job)
                except Exception:
                    pass
                try:
                    self.player.stop()
                except Exception:
                    pass
                self.destroy()

        # create and show player window
        try:
            pw = PlayerWindow(self, media_url, title=item.title)
        except Exception as e:
            messagebox.showerror('Player error', f'Failed to open embedded player: {e}')


    def play_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        item = self.iid_map.get(iid)
        if not item:
            return
        if not has_vlc:
            msg = (
                "Embedded playback requires VLC (libVLC) and the python-vlc package.\n\n"
                "To enable embedded playback, run these steps in PowerShell:\n"
                "1) Install system VLC (https://www.videolan.org/vlc/) or via winget:\n"
                "   winget install --id=VideoLAN.VLC -e\n"
                "2) Install python-vlc in your Python environment:\n"
                "   python -m pip install python-vlc\n\n"
                "After installing, restart this application.\n\n"
                "If you prefer not to install, you can still play the video in your web browser.")
            if messagebox.askyesno('Embedded player not available', msg + '\n\nOpen in browser instead?'):
                import webbrowser
                webbrowser.open(item.url)
            return

        # Directly open player window with default quality (embedded dropdown will handle switching)
        self._open_player_window_with_url(item, item.url)

    def _open_player_window_with_url(self, item: DownloadItem, url: str):
        # Like _open_player_window but allows custom stream URL
        if not has_vlc:
            messagebox.showerror('Player missing', 'Embedded player requires python-vlc and libvlc installed. Please install VLC and the python-vlc package.')
            return
        media_url = url
        class PlayerWindow(tk.Toplevel):
            def __init__(self, master, media_url, title=None):
                super().__init__(master)
                self.title(title or 'Player')
                self.geometry('960x600')
                self.protocol('WM_DELETE_WINDOW', self._on_close)
                # Disable hardware acceleration where possible to avoid driver/codec issues
                try:
                    inst = self._create_vlc_instance()
                    if inst is not None:
                        self.instance = inst
                    else:
                        self.instance = vlc.Instance()
                except Exception:
                    try:
                        self.instance = vlc.Instance()
                    except Exception:
                        self.instance = None
                self.player = self.instance.media_player_new()
                # Load available formats (labels + format ids) for the video
                try:
                    ydl_opts = {'quiet': True, 'no_warnings': True}
                    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(item.url, download=False)
                    fmts = info.get('formats') or []
                    # collect (height, has_audio, label, format_id, url)
                    collected = []
                    seen_labels = set()
                    for f in fmts:
                        if not f.get('url'):
                            continue
                        fmt_id = f.get('format_id') or f.get('format') or ''
                        vcodec = f.get('vcodec')
                        acodec = f.get('acodec')
                        note = (f.get('format_note') or '').strip()
                        height = f.get('height')
                        try:
                            height_int = int(height) if height else 0
                        except Exception:
                            height_int = 0
                        fps = f.get('fps') or ''
                        ext = f.get('ext') or ''
                        parts = []
                        if note:
                            parts.append(note)
                        if height_int:
                            parts.append(f"{height_int}p")
                        if fps:
                            parts.append(f"{fps}fps")
                        if ext:
                            parts.append(ext)
                        label = ' '.join(parts).strip() or fmt_id or 'unknown'
                        has_video = bool(vcodec and vcodec != 'none')
                        has_audio = bool(acodec and acodec != 'none')
                        if has_video and not has_audio:
                            label = f"{label} (video-only)"
                        out_fmt = fmt_id
                        if has_video and not has_audio and fmt_id:
                            out_fmt = f"{fmt_id}+bestaudio"
                        is_hls = 'hls' in (f.get('protocol') or '').lower() or f.get('ext') == 'm3u8'
                        # deduplicate by canonical label (e.g., '1080p') to avoid duplicates
                        key = label.split(' ')[0]
                        if key in seen_labels:
                            continue
                        seen_labels.add(key)
                        collected.append((height_int, 1 if has_audio else 0, 1 if is_hls else 0, label, out_fmt, f.get('url')))
                    # sort by height desc then audio presence, then HLS preference
                    collected.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
                    self.quality_options = [c[3] for c in collected]
                    self.quality_format_ids_local = [c[4] for c in collected]
                    self.quality_urls_local = [c[5] for c in collected]
                    # log top entries for debugging
                    try:
                        tops = ', '.join(self.quality_options[:6])
                        self.master.event_q.put(('log', f'Player formats: {tops}'))
                    except Exception:
                        pass
                except Exception:
                    self.quality_options = ['No formats']
                    self.quality_format_ids_local = [media_url]
                    self.quality_urls_local = [media_url]

                # Video area
                self.video_frame = ttk.Frame(self)
                self.video_frame.pack(fill='both', expand=True)
                self.canvas = tk.Canvas(self.video_frame, bg='black', highlightthickness=0)
                self.canvas.pack(fill='both', expand=True)

                # Controls area
                ctrl = ttk.Frame(self)
                ctrl.pack(fill='x', pady=8)

                # Play / Pause
                self.play_state = tk.StringVar(value='Play')
                self.play_btn = ttk.Button(ctrl, textvariable=self.play_state, command=self.toggle_play)
                self.play_btn.grid(row=0, column=0, padx=8)

                # Rewind / Forward
                self.rew_btn = ttk.Button(ctrl, text='⏪', command=lambda: self.skip(-10))
                self.rew_btn.grid(row=0, column=1, padx=4)
                self.fwd_btn = ttk.Button(ctrl, text='⏩', command=lambda: self.skip(10))
                self.fwd_btn.grid(row=0, column=2, padx=4)

                # Quality dropdown (embedded in player)
                ttk.Label(ctrl, text='Quality:').grid(row=0, column=3, padx=(12,4))
                self.quality_var_local = tk.StringVar()
                self.quality_combo_local = ttk.Combobox(ctrl, textvariable=self.quality_var_local, values=self.quality_options, state='readonly', width=30)
                self.quality_combo_local.grid(row=0, column=4, padx=4)
                if self.quality_options:
                    try:
                        self.quality_combo_local.set(self.quality_options[0])
                    except Exception:
                        pass
                self.quality_combo_local.bind('<<ComboboxSelected>>', self.on_quality_change)

                # Time / progress
                self.time_lbl = ttk.Label(ctrl, text='00:00 / 00:00')
                self.time_lbl.grid(row=0, column=5, padx=(12,4))
                self.pos_var = tk.DoubleVar()
                self.slider = ttk.Scale(ctrl, orient='horizontal', from_=0, to=1000, variable=self.pos_var, command=self.on_seek)
                self.slider.grid(row=0, column=6, sticky='ew', padx=4)
                ctrl.columnconfigure(6, weight=1)

                # Volume
                vol_lbl = ttk.Label(ctrl, text='🔊')
                vol_lbl.grid(row=0, column=7, padx=(8,4))
                self.vol_var = tk.DoubleVar(value=100)
                vol_slider = ttk.Scale(ctrl, orient='horizontal', from_=0, to=100, variable=self.vol_var, command=self.on_vol, length=100)
                vol_slider.grid(row=0, column=8, padx=4)

                # Fullscreen
                self.full_btn = ttk.Button(ctrl, text='⛶', command=self.toggle_fullscreen)
                self.full_btn.grid(row=0, column=9, padx=(12,8))

                self._fullscreen = False
                self._updating = False
                self._is_playing = False

                # load media but do NOT autoplay; user must press Play
                try:
                    # prefer using the direct URL if available, else format id
                    if getattr(self, 'quality_urls_local', None) and self.quality_urls_local:
                        self.set_media(self.quality_urls_local[0])
                    elif getattr(self, 'quality_format_ids_local', None) and self.quality_format_ids_local:
                        self.set_media(self.quality_format_ids_local[0])
                    else:
                        self.set_media(media_url)
                except Exception:
                    try:
                        self.set_media(media_url)
                    except Exception:
                        pass

                try:
                    try:
                        self.attributes('-topmost', True)
                        self.lift()
                        self.focus_force()
                        self.after(300, lambda: self.attributes('-topmost', False))
                    except Exception:
                        pass
                except Exception:
                    pass
                self.after(600, self._set_hwnd)
                self._job = self.after(500, self.update_position)

            def set_media(self, url):
                try:
                    stream = None
                    # If url already looks like a direct stream, use it
                    if isinstance(url, str) and url.startswith(('http://', 'https://')):
                        # But guard against thumbnail/storyboard URLs (i.ytimg.com/...)
                        ul = (url or '').lower()
                        if ul.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in ul or 'storyboard' in ul or 'googleusercontent' in ul:
                            try:
                                self.master.event_q.put(('log', f'set_media: provided url looks like image/storyboard, skipping direct use: {url[:200]}'))
                            except Exception:
                                pass
                            stream = None
                        else:
                            stream = url
                    else:
                        fmt_expr = url
                        # First try resolving format expression via yt-dlp (map format -> direct URL)
                        try:
                            ydl_opts = {'quiet': True, 'no_warnings': True}
                            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                                info = None
                                try:
                                    # ask yt-dlp to return the format-specific info/url
                                    info = ydl.extract_info(item.url, download=False, format=fmt_expr)
                                except Exception:
                                    # fallback: extract entire info and search formats manually
                                    info = ydl.extract_info(item.url, download=False)
                                if info:
                                    direct = info.get('url')
                                    if direct:
                                        # Guard against cases where yt-dlp returns a thumbnail/storyboard
                                        dlw = (direct or '').lower()
                                        if not (dlw.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in dlw or 'storyboard' in dlw or 'googleusercontent' in dlw):
                                            stream = direct
                                        else:
                                            try:
                                                self.master.event_q.put(('log', f'set_media: extracted direct url looks like image, skipping: {direct[:200]}'))
                                            except Exception:
                                                pass
                                    else:
                                        fmts = info.get('formats') or []
                                        if fmts:
                                            # collect scored candidates and prefer audio+video streams
                                            cand = []
                                            for f in fmts:
                                                fu = f.get('url')
                                                if not fu:
                                                    continue
                                                ful = (fu or '').lower()
                                                # skip image/storyboard urls returned as formats
                                                if ful.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in ful or 'storyboard' in ful or 'googleusercontent' in ful:
                                                    try:
                                                        self.master.event_q.put(('log', f'set_media: skipping image-like format.url: {fu[:200]}'))
                                                    except Exception:
                                                        pass
                                                    continue
                                                vcodec = f.get('vcodec')
                                                acodec = f.get('acodec')
                                                score = 0
                                                if vcodec and vcodec != 'none':
                                                    score += 2
                                                if acodec and acodec != 'none':
                                                    score += 2
                                                if f.get('ext') == 'mp4':
                                                    score += 1
                                                cand.append((score, fu, f))
                                            if cand:
                                                cand.sort(key=lambda x: x[0], reverse=True)
                                                # pick highest scored candidate
                                                stream = cand[0][1]
                                                try:
                                                    best_fmt = cand[0][2]
                                                    self.master.event_q.put(('log', f'set_media: selected format from formats list ext={best_fmt.get("ext")} proto={best_fmt.get("protocol")} url={(stream[:200] + "...") if len(stream)>200 else stream}'))
                                                except Exception:
                                                    pass
                        except Exception as e:
                            try:
                                self.master.event_q.put(('log', f'Failed to resolve format {fmt_expr}: {e}'))
                            except Exception:
                                pass

                        # if still unresolved, try the app-level resolver which prefers HLS/progressive
                        if not stream:
                            try:
                                class _Tmp:
                                    pass
                                t = _Tmp()
                                t.url = getattr(self, '_original_media_url', None) or item.url
                                resolved = self.master._resolve_stream_url(t)
                                if resolved:
                                    stream = resolved
                            except Exception as e:
                                try:
                                    self.master.event_q.put(('log', f'Fallback stream resolve failed: {e}'))
                                except Exception:
                                    pass

                    if not stream:
                        # last resort: use the provided media_url (may trigger VLC youtube.lua)
                        stream = media_url

                    # Log resolved stream (trim long strings)
                    try:
                        short = stream if not stream or len(stream) < 240 else stream[:240] + '...'
                        self.master.event_q.put(('log', f'Player resolved stream: {short}'))
                    except Exception:
                        pass

                    self.media = self.instance.media_new(stream)
                    try:
                        self.media.add_option(':avcodec-hw=none')
                        self.media.add_option(':hwdec=0')
                        self.media.add_option(':no-video-title-show')
                        self.media.add_option(':network-caching=2000')
                        self.media.add_option(':direct3d11-hw-blending=no')
                        self.media.add_option(':avcodec-threads=1')
                    except Exception:
                        pass
                    self.player.set_media(self.media)
                    self.play_state.set('Play')
                    self._is_playing = False
                except Exception as e:
                    try:
                        self.master.event_q.put(('log', f'set_media failed: {e}'))
                    except Exception:
                        pass

            def on_quality_change(self, event=None):
                idx = self.quality_combo_local.current()
                if idx < 0:
                    return
                # determine selected format expression
                try:
                    fmt_expr = self.quality_urls_local[idx] if self.quality_urls_local[idx] else self.quality_format_ids_local[idx]
                except Exception:
                    return
                # preserve current timestamp (milliseconds)
                try:
                    cur_pos = self.player.get_time()
                except Exception:
                    cur_pos = 0
                try:
                    self.player.stop()
                except Exception:
                    pass
                # set new stream url and play
                try:
                    self.set_media(fmt_expr)
                    # small delay then seek and play
                    def seek_and_play():
                        try:
                            if cur_pos and cur_pos > 0:
                                self.player.play()
                                # allow more time for media to be ready
                                self.after(1000, lambda: self.player.set_time(cur_pos))
                                self.play_state.set('Pause')
                                self._is_playing = True
                            else:
                                self.player.play()
                                self.play_state.set('Pause')
                                self._is_playing = True
                        except Exception:
                            pass
                    self.after(1000, seek_and_play)  # increased delay
                except Exception:
                    pass

            def toggle_fullscreen(self):
                self._fullscreen = not self._fullscreen
                self.attributes('-fullscreen', self._fullscreen)

            def _set_hwnd(self):
                try:
                    hwnd = self.canvas.winfo_id()
                    if sys.platform.startswith('win'):
                        self.player.set_hwnd(hwnd)
                    elif sys.platform.startswith('linux'):
                        self.player.set_xwindow(hwnd)
                    elif sys.platform.startswith('darwin'):
                        self.player.set_nsobject(hwnd)
                except Exception:
                    pass

            def toggle_play(self):
                try:
                    if self.player.is_playing():
                        self.player.pause()
                        self.play_state.set('Play')
                        self._is_playing = False
                    else:
                        self.player.play()
                        self.player.audio_set_volume(int(self.vol_var.get()))
                        self.play_state.set('Pause')
                        self._is_playing = True
                except Exception as e:
                    self.master.log('Player error:', e)

            def stop(self):
                try:
                    self.player.stop()
                except Exception:
                    pass
                self.play_state.set('Play')
                self._is_playing = False

            def skip(self, seconds):
                try:
                    length = self.player.get_length() / 1000.0
                    cur = self.player.get_time() / 1000.0
                    target = max(0, min(length, cur + seconds))
                    self.player.set_time(int(target * 1000))
                except Exception:
                    pass

            def on_seek(self, val):
                if self._updating:
                    return
                try:
                    pos = float(val) / 1000.0
                    length = self.player.get_length()
                    if length > 0:
                        new_time = int(pos * length)
                        self.player.set_time(new_time)
                except Exception:
                    pass

            def on_vol(self, val):
                try:
                    self.player.audio_set_volume(int(float(val)))
                except Exception:
                    pass

            def update_position(self):
                try:
                    length = self.player.get_length()
                    if length > 0:
                        cur = self.player.get_time()
                        self._updating = True
                        try:
                            pos = cur / length
                            self.pos_var.set(pos * 1000)
                        finally:
                            self._updating = False
                        def fmt(ms):
                            s = int(ms/1000)
                            m, s = divmod(s, 60)
                            h, m = divmod(m, 60)
                            if h:
                                return f"{h:02d}:{m:02d}:{s:02d}"
                            return f"{m:02d}:{s:02d}"
                        self.time_lbl.config(text=f"{fmt(cur)} / {fmt(length)}")
                except Exception:
                    pass
                self._job = self.after(500, self.update_position)

            def _on_close(self):
                try:
                    if self._job:
                        self.after_cancel(self._job)
                except Exception:
                    pass
                try:
                    self.player.stop()
                except Exception:
                    pass
                self.destroy()
        try:
            pw = PlayerWindow(self, media_url, title=item.title)
        except Exception as e:
            messagebox.showerror('Player error', f'Failed to open embedded player: {e}')

    def _open_preview_for_entry(self, entry: dict):
        """Open a preview/player for a search-result entry. Resolve a direct stream when possible."""
        try:
            url = entry.get('webpage_url') or f"https://www.youtube.com/watch?v={entry.get('id')}"
            it = DownloadItem(url, fmt='mp4')
            it.title = entry.get('title') or url
            it.thumbnail = entry.get('thumbnail')
            # Try to resolve to a direct playable stream URL using yt-dlp
            resolved = None
            if youtube_dl is not None:
                try:
                    resolved = self._resolve_stream_url(it)
                except Exception:
                    resolved = None
            # If a Search Results dialog has a grab, release it so the player can appear above it
            try:
                dlg = getattr(self, '_last_search_dialog', None)
                if dlg is not None:
                    try:
                        dlg.grab_release()
                    except Exception:
                        pass
            except Exception:
                pass

            # Quick check: ensure resolved URL (or page URL) is reachable before embedding.
            # If resolved looks like an image/storyboard (i.ytimg.com...), ignore it and use the canonical page URL
            playable_url = resolved or it.url
            try:
                plu = (playable_url or '').lower()
                if plu.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in plu or 'storyboard' in plu or 'googleusercontent' in plu:
                    try:
                        self.event_q.put(('log', f'Preview: resolved playable_url looks like image/storyboard, using page URL instead: {playable_url[:160]}'))
                    except Exception:
                        pass
                    playable_url = it.url
                    resolved = None
            except Exception:
                pass
            reachable = False
            try:
                import urllib.request
                req = urllib.request.Request(playable_url, headers={'User-Agent': 'yt-downloader-preview/1.0'})
                with urllib.request.urlopen(req, timeout=6) as resp:
                    # consider reachable if we get any 2xx/3xx response and can read a few bytes
                    code = getattr(resp, 'status', None) or getattr(resp, 'getcode', lambda: None)()
                    if code and (200 <= int(code) < 400):
                        try:
                            _ = resp.read(64)
                        except Exception:
                            pass
                        reachable = True
            except Exception:
                reachable = False

            if has_vlc and reachable:
                # prefer resolved direct stream when available
                try:
                    self._open_player_window_with_url(it, playable_url)
                    return
                except Exception:
                    pass

            # fallback: try to open in external VLC if installed, else open in browser
            try:
                import shutil, subprocess, webbrowser
                vlc_path = shutil.which('vlc') or shutil.which('cvlc')
                # common Windows path
                if not vlc_path:
                    p = r"C:\Program Files\VideoLAN\VLC\vlc.exe"
                    if os.path.exists(p):
                        vlc_path = p
                if vlc_path:
                    try:
                        subprocess.Popen([vlc_path, playable_url])
                        return
                    except Exception:
                        pass
                webbrowser.open(playable_url)
                return
            except Exception:
                try:
                    import webbrowser
                    webbrowser.open(playable_url)
                    return
                except Exception:
                    pass
        except Exception as e:
            try:
                self.event_q.put(('log', f'Preview open failed: {e}'))
            except Exception:
                pass
            try:
                messagebox.showerror('Preview error', f'Failed to open preview: {e}')
            except Exception:
                pass

    def on_format_change(self):
        # no-op for now; mp3 bitrate remains available
        pass

    def choose_output_dir(self):
        d = filedialog.askdirectory(initialdir=self.output_dir.get())
        if d:
            self.output_dir.set(d)

    def _find_ffmpeg(self):
        try:
            path = shutil.which('ffmpeg')
            if path:
                return path
        except Exception:
            pass
        return None

    def _create_vlc_instance(self):
        """Create a libVLC Instance with a set of fallback options to improve Windows video output compatibility.

        Returns a vlc.Instance or None if vlc module is unavailable.
        """
        if not has_vlc or vlc is None:
            return None
        # Try several vout options that are known to be more compatible on Windows.
        # Try safer vout options first (win32/gdi), leave direct3d11 last as fallback
        option_sets = [
            ['--avcodec-hw=none', '--no-video-title-show', '--vout=win32', '--direct3d11-hw-blending=no'],
            ['--avcodec-hw=none', '--no-video-title-show', '--vout=direct3d', '--direct3d11-hw-blending=no'],
            ['--avcodec-hw=none', '--no-video-title-show', '--vout=direct3d11', '--direct3d11-hw-blending=no'],
            ['--avcodec-hw=none', '--no-video-title-show', '--direct3d11=no'],
            ['--avcodec-hw=none', '--no-video-title-show'],
            ['--no-video-title-show']
        ]
        for opts in option_sets:
            try:
                # vlc.Instance accepts a string or sequence; join to safe single string
                arg = ' '.join(opts)
                inst = vlc.Instance(arg)
                try:
                    self.event_q.put(('log', f'Created VLC instance with options: {arg}'))
                except Exception:
                    pass
                return inst
            except Exception:
                continue
        # final fallback
        try:
            inst = vlc.Instance()
            return inst
        except Exception:
            return None

    def choose_ffmpeg(self):
        # Ask user to select ffmpeg executable
        try:
            p = filedialog.askopenfilename(title='Select ffmpeg executable', filetypes=[('ffmpeg exe','ffmpeg.exe'), ('All','*.*')])
            if p:
                self.ffmpeg_path = p
                self.log('ffmpeg set to:', p)
                self.status_var.set(f'ffmpeg: {Path(p).name}')
        except Exception as e:
            self.log('Failed to set ffmpeg:', e)

    def log(self, *parts):
        t = ' '.join(str(p) for p in parts)
        timestamp = time.strftime('%H:%M:%S')
        self.log_text.insert('end', f"[{timestamp}] {t}\n")
        self.log_text.see('end')

    def start_downloads(self):
        if youtube_dl is None:
            messagebox.showerror('Missing dependency', "The Python package 'yt-dlp' is not installed. Run: python -m pip install -r requirements.txt")
            return
        if not self.queue_items:
            messagebox.showinfo('Queue empty', 'Add at least one URL to the queue.')
            return
        if self.work_thread and self.work_thread.is_alive():
            messagebox.showinfo('Already running', 'Download worker is already running.')
            return
        # if any queued item requires ffmpeg (mp3 conversion), ensure ffmpeg is available
        need_ffmpeg = any(it.fmt == 'mp3' for it in self.queue_items)
        if need_ffmpeg and not self.ffmpeg_path:
            msg = (
                "FFmpeg is required to convert audio to MP3 but was not found on your system.\n\n"
                "Options:\n"
                "1) Install ffmpeg and add it to your PATH (recommended).\n"
                "   Example (PowerShell): winget install --id=Gyan.FFmpeg -e or download from https://ffmpeg.org/\n"
                "2) Or click 'Set ffmpeg' to point this app to your ffmpeg.exe file.\n\n"
                "Open settings now?"
            )
            if messagebox.askyesno('FFmpeg not found', msg):
                # focus the app so user can click Set ffmpeg
                try:
                    self.lift()
                except Exception:
                    pass
            return
        self.stop_requested.clear()
        # Ensure quality for MP4 items
        for it in self.queue_items:
            if it.fmt == 'mp4':
                if hasattr(self, 'quality_combo') and self.quality_combo.get() and self.quality_combo.get() not in ['Auto', 'No formats', 'Fetching...', 'yt-dlp missing']:
                    try:
                        idx = list(self.quality_combo['values']).index(self.quality_combo.get())
                        it.quality = self.quality_format_ids[idx]
                        it.quality_label = self.quality_combo.get()
                    except Exception:
                        pass
        # Show download summary
        summary = []
        total_size = 0.0
        for i, it in enumerate(self.queue_items, 1):
            size_str = self._estimate_size(it)
            quality = getattr(it, 'quality_label', getattr(it, 'quality', 'default')) or 'default'
            if size_str != 'Unknown':
                try:
                    size_mb = float(size_str)
                    total_size += size_mb
                    size_display = f"{size_mb:.2f} MB"
                except:
                    size_display = size_str
            else:
                size_display = size_str
            summary.append(f"{i}. {it.title or it.url} - {quality} - {size_display}")
        total_display = f"{total_size:.2f} MB" if total_size > 0 else "Unknown"
        msg = f"Download Summary:\n\n" + '\n'.join(summary) + f"\n\nTotal estimated size: {total_display}\n\nProceed?"
        if not messagebox.askyesno('Confirm Download', msg):
            return
        self.work_thread = threading.Thread(target=self._worker_main, daemon=True)
        self.work_thread.start()
        self.status_var.set('Downloading...')
        self.log('Started downloads')

    def _estimate_size(self, item):
        try:
            ydl_opts = {'quiet': True, 'no_warnings': True}
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                # Try to get general info first
                info = None
                try:
                    info = ydl.extract_info(item.url, download=False)
                except Exception as e:
                    try:
                        self.event_q.put(('log', f'Initial extract_info failed: {e}'))
                    except Exception:
                        pass
                    info = None

                if item.fmt == 'mp3':
                    # For MP3, estimate based on duration and bitrate using general info
                    if not info:
                        return 'Unknown'
                    duration = info.get('duration', 0)
                    self.event_q.put(('log', f'Duration raw: {duration} type: {type(duration)}'))
                    try:
                        duration = float(duration) if duration else 0
                    except Exception as e:
                        self.event_q.put(('log', f'Duration convert failed: {e}'))
                        duration = 0
                    bitrate = getattr(item, 'bitrate', 192)
                    try:
                        bitrate = int(bitrate) * 1000
                    except Exception:
                        bitrate = 192 * 1000
                    if duration and bitrate:
                        size_bytes = (duration * bitrate) / 8
                        self.event_q.put(('log', f'Estimated MP3 size: {size_bytes / 1e6:.2f} MB'))
                        return f"{size_bytes / 1e6:.2f}"
                    return 'Unknown'
                else:
                    # For MP4, if the user selected a specific format, query that format to get accurate filesize
                    fmt = getattr(item, 'quality', None)
                    info_fmt = None
                    try:
                        if fmt:
                            info_fmt = ydl.extract_info(item.url, download=False, format=fmt)
                    except Exception as e:
                        try:
                            self.event_q.put(('log', f'Format-specific extract_info failed for {fmt}: {e}'))
                        except Exception:
                            pass
                        info_fmt = None

                    info_to_use = info_fmt or info
                    if not info_to_use:
                        return 'Unknown'

                    size = info_to_use.get('filesize') or info_to_use.get('filesize_approx')
                    if size is not None:
                        try:
                            size_mb = int(size) / 1e6
                            self.event_q.put(('log', f'Got size: {size_mb:.2f} MB'))
                            return f"{size_mb:.2f}"
                        except (ValueError, TypeError) as e:
                            try:
                                self.event_q.put(('log', f'Filesize parse failed: {e} value={size}'))
                            except Exception:
                                pass

                    # Fallback: estimate from duration and avg bitrate
                    duration = info_to_use.get('duration', 0)
                    self.event_q.put(('log', f'Duration raw MP4: {duration} type: {type(duration)}'))
                    try:
                        duration = float(duration) if duration else 0
                    except Exception as e:
                        self.event_q.put(('log', f'Duration convert failed MP4: {e}'))
                        duration = 0
                    if duration:
                        # Assume 5 Mbps for HD video
                        bitrate = 5e6  # 5 Mbps
                        size_bytes = (duration * bitrate) / 8
                        size_mb = size_bytes / 1e6
                        self.event_q.put(('log', f'Fallback size estimate: {size_mb:.2f} MB'))
                        return f"{size_mb:.2f}"
                    return 'Unknown'
        except Exception as e:
            try:
                self.event_q.put(('log', f'Size estimate failed: {e}'))
            except Exception:
                pass
            return 'Unknown'

    def stop_downloads(self):
        if self.work_thread and self.work_thread.is_alive():
            self.stop_requested.set()
            self.log('Stop requested — waiting for current download to finish/cancel...')
            self.status_var.set('Stopping...')
        else:
            self.log('No worker running')

    def remove_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        # confirm with countdown
        self._confirm_then_execute(f"Remove selected item?", 5, lambda: self._do_remove(iid))

    def _do_remove(self, idx):
        # idx may be iid (string) or numeric index
        try:
            if isinstance(idx, str):
                iid = idx
                item = self.iid_map.pop(iid, None)
                if item and item in self.queue_items:
                    self.queue_items.remove(item)
                try:
                    self.tree.delete(iid)
                except Exception:
                    pass
                self.log('Removed item', iid)
            else:
                # numeric index
                try:
                    item = self.queue_items.pop(idx)
                except Exception:
                    item = None
                # remove by matching iid if present
                to_del = None
                for k, v in list(self.iid_map.items()):
                    if v is item:
                        to_del = k
                        break
                if to_del:
                    try:
                        self.tree.delete(to_del)
                    except Exception:
                        pass
                    self.iid_map.pop(to_del, None)
                self.log('Removed item at index', idx)
        except Exception as e:
            self.log('Remove failed:', e)

    def clear_queue(self):
        if not self.queue_items:
            return
        self._confirm_then_execute('Clear entire queue?', 5, self._do_clear_queue)

    def _do_clear_queue(self):
        try:
            for iid in list(self.iid_map.keys()):
                try:
                    self.tree.delete(iid)
                except Exception:
                    pass
            self.iid_map.clear()
            self.queue_items.clear()
            self.log('Queue cleared')
        except Exception as e:
            self.log('Clear failed:', e)

    def _confirm_then_execute(self, message: str, delay_seconds: int, action):
        # Toplevel confirmation with countdown and Cancel button
        dlg = tk.Toplevel(self)
        dlg.title('Confirm')
        dlg.geometry('360x120')
        dlg.transient(self)
        dlg.grab_set()
        lbl = ttk.Label(dlg, text=message)
        lbl.pack(pady=(12, 6))
        countdown_var = tk.StringVar(value=f'Executing in {delay_seconds} s...')
        c_lbl = ttk.Label(dlg, textvariable=countdown_var)
        c_lbl.pack()
        canceled = {'v': False}

        def on_cancel():
            canceled['v'] = True
            dlg.destroy()
            self.log('Operation canceled')

        btn = ttk.Button(dlg, text='Cancel', command=on_cancel)
        btn.pack(pady=8)

        # run countdown
        def tick(remaining):
            if canceled['v']:
                return
            if remaining <= 0:
                try:
                    dlg.destroy()
                except Exception:
                    pass
                try:
                    action()
                except Exception as e:
                    self.log('Action failed:', e)
                return
            countdown_var.set(f'Executing in {remaining} s...')
            dlg.after(1000, lambda: tick(remaining - 1))

        tick(delay_seconds)

    def open_output_folder(self, *_):
        out = Path(self.output_dir.get())
        if not out.exists():
            messagebox.showwarning('Not found', f'Output folder does not exist: {out}')
            return
        try:
            if sys.platform.startswith('win'):
                os.startfile(out)
            else:
                import subprocess
                subprocess.run(['xdg-open', str(out)])
        except Exception as e:
            self.log('Failed to open folder:', e)

    def on_open_output(self, event=None):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        it = self.iid_map.get(iid)
        if it.filename:
            try:
                if sys.platform.startswith('win'):
                    os.startfile(Path(it.filename).parent)
                else:
                    import subprocess
                    subprocess.run(['xdg-open', str(Path(it.filename).parent)])
            except Exception as e:
                self.log('Failed to open output folder:', e)

    def _worker_main(self):
        # Process items sequentially
        for idx, item in enumerate(list(self.queue_items)):
            if self.stop_requested.is_set():
                self.event_q.put(('status', 'Stopped'))
                break
            self._set_item_status(idx, 'Downloading')
            try:
                self._download_item(idx, item)
            except Exception as e:
                item.status = 'Error'
                item.error = str(e)
                self.log(f'Error for {item.url}:', e)
                self._set_item_status(idx, 'Error')
            # small delay to let UI update
            time.sleep(0.1)
        self.event_q.put(('done', None))

    # --- helper methods: fetch title and search ---
    def _fetch_title_and_update(self, idx, item: DownloadItem):
        try:
            info = self._fetch_info(item.url)
            title = info.get('title') if info else None
            item.title = title or item.url
            # fetch thumbnail URL if available
            item.thumbnail = info.get('thumbnail') if info else None
            # set status to Standby when metadata fetch completes
            item.status = 'Standby'
            self.event_q.put(('log', f'Got title: {item.title}'))
            # After title/metadata is fetched, populate toolbar quality options
            try:
                if self.format_var.get() == 'mp4':
                    # run background fetch to populate quality combobox for this URL
                    try:
                        self._fetch_formats_background(item.url)
                    except Exception:
                        pass
            except Exception:
                pass
            # update tree entry by iid if present
            iid = None
            for k, v in list(self.iid_map.items()):
                if v is item:
                    iid = k
                    break
            if iid:
                self.event_q.put(('update_item', (iid, (item.title, item.status, self._format_progress_bar(item.progress)))))
                # fetch thumbnail immediately after title
                threading.Thread(target=self._fetch_thumbnail, args=(item, iid), daemon=True).start()
            else:
                # fallback to index-based update
                self.event_q.put(('update_item', (idx, f"{item.title} — {item.status}")))
        except Exception as e:
            item.title = item.url
            self.event_q.put(('log', f'Failed to fetch title for {item.url}: {e}'))
            self.event_q.put(('update_item', (idx, f"{item.url} — {item.status}")))

    def _fetch_info(self, url):
        # wrapper to extract metadata without downloading
        try:
            ydl_opts = {'quiet': True, 'no_warnings': True}
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception:
            return None

    def _fetch_formats_background(self, url):
        """Background fetch formats for a URL and update toolbar combobox and format-id mapping."""
        if not url:
            return
        try:
            self.event_q.put(('log', f'_fetch_formats_background: start for {url}'))
        except Exception:
            pass

        def worker(u):
            # collect (score/height, label, out_format_id, url)
            collected = []
            try:
                ydl_opts = {'quiet': True, 'no_warnings': True, 'format_sort': ['res', 'ext', 'acodec', 'vcodec']}
                with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(u, download=False)
                fmts = info.get('formats') or []
                try:
                    self.event_q.put(('log', f'yt-dlp returned {len(fmts)} formats for {u}'))
                except Exception:
                    pass
                seen_labels = set()
                for f in fmts:
                    if not f.get('url'):
                        continue
                    fmt_id = f.get('format_id') or f.get('format') or ''
                    vcodec = f.get('vcodec')
                    acodec = f.get('acodec')
                    note = (f.get('format_note') or '').strip()
                    height = f.get('height')
                    try:
                        height_int = int(height) if height else 0
                    except Exception:
                        height_int = 0
                    ext = f.get('ext') or ''
                    # Prefer compact labels: use height (e.g., '1080p') when available,
                    # otherwise use format_note or file extension or format id.
                    if height_int:
                        label = f"{height_int}p"
                    elif note:
                        label = note
                    elif ext:
                        label = ext
                    else:
                        label = fmt_id or 'unknown'
                    # mark video-only formats
                    has_video = bool(vcodec and vcodec != 'none')
                    has_audio = bool(acodec and acodec != 'none')
                    if has_video and not has_audio:
                        label = f"{label} (video-only)"
                    # de-dup
                    if label in seen_labels:
                        continue
                    seen_labels.add(label)
                    # choose output format id: if video-only, use video+best audio for downloads
                    out_fmt = fmt_id
                    if has_video and not has_audio and fmt_id:
                        out_fmt = f"{fmt_id}+bestaudio"
                    # store tuple for sorting by height then by presence of audio
                    score = (height_int, 1 if has_audio else 0)
                    collected.append((score, label, out_fmt, f.get('url')))
            except Exception as exc:
                try:
                    self.event_q.put(('log', f'Format fetch failed for {url}: {exc}'))
                except Exception:
                    pass

            # sort collected by height desc, audio presence first
            collected.sort(key=lambda x: (x[0][0], x[0][1]), reverse=True)
            qualities = [c[1] for c in collected]
            format_ids = [c[2] for c in collected]
            urls_for_play = [c[3] for c in collected]
            try:
                self.event_q.put(('log', f'Collected {len(qualities)} qualities: {qualities[:5]}'))
            except Exception:
                pass
            if not qualities:
                qualities = ['No formats']
                format_ids = []
                urls_for_play = []

            def apply_ui():
                try:
                    if not getattr(self, 'quality_combo', None):
                        # create combo if missing
                        fmt_frame = None
                        # try to find the fmt_frame by walking children (best-effort)
                        for child in self.winfo_children():
                            if isinstance(child, ttk.Frame):
                                fmt_frame = child
                                break
                    try:
                        self.quality_combo['values'] = qualities
                        try:
                            self.quality_combo.set(qualities[0])
                        except Exception:
                            pass
                        # apply default selection to state so Confirm uses it
                        try:
                            default_label = qualities[0]
                            self.default_quality_label = default_label
                            if format_ids:
                                self.default_quality = format_ids[0]
                            # if a tree item is selected and matches this URL, set its quality
                            sel = None
                            try:
                                sel = self.tree.selection()
                            except Exception:
                                sel = None
                            if sel:
                                try:
                                    iid = sel[0]
                                    itm = self.iid_map.get(iid)
                                    if itm and getattr(itm, 'url', '') == url:
                                        itm.quality = getattr(self, 'default_quality', None)
                                        itm.quality_label = default_label
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        self.quality_format_ids = format_ids
                        try:
                            self.quality_combo.pack(side='left', padx=(8,4))
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    pass

            try:
                self.event_q.put(('show_dialog', apply_ui))
            except Exception:
                apply_ui()

        threading.Thread(target=worker, args=(url,), daemon=True).start()

    def _on_toolbar_quality_change(self):
        """Handle toolbar quality selection: apply to selected queue item or set default for future items."""
        try:
            val = self.quality_combo.get()
        except Exception:
            val = None
        fmt = None
        try:
            vals = list(self.quality_combo['values'])
            if val in vals:
                idx = vals.index(val)
                fmt = self.quality_format_ids[idx] if getattr(self, 'quality_format_ids', None) and idx < len(self.quality_format_ids) else None
        except Exception:
            pass
        # apply to currently selected item if present
        try:
            sel = self.tree.selection()
            if sel:
                iid = sel[0]
                itm = self.iid_map.get(iid)
                if itm:
                    itm.quality = fmt
                    itm.quality_label = val or itm.quality_label
                    return
        except Exception:
            pass
        # otherwise set defaults for future items
        try:
            if fmt:
                self.default_quality = fmt
            if val:
                self.default_quality_label = val
        except Exception:
            pass

    def _resolve_stream_url(self, item: DownloadItem):
        """Use yt-dlp to find a direct playable media URL for the given item.

        Prefer a progressive format (audio+video) or HLS/M3U8 that libVLC can play.
        Return None if resolution failed.
        """
        try:
            ydl_opts = {'quiet': True, 'no_warnings': True}
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(item.url, download=False)
        except Exception as e:
            self.log('yt-dlp extract_info failed:', e)
            return None

        if not info:
            return None

        # If extract_info provides a playable url directly, try it first
        direct = info.get('url')
        proto = info.get('protocol') or ''
        if direct and proto and proto.startswith(('http', 'https', 'm3u8')):
            # guard: sometimes yt-dlp/info.url may contain a thumbnail/storyboard image (ytimg)
            dlow = (direct or '').lower()
            if not (dlow.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in dlow or 'storyboard' in dlow or 'googleusercontent' in dlow):
                return direct
            else:
                try:
                    self.event_q.put(('log', f'_resolve_stream_url: info.url appears to be an image, skipping: {direct[:200]}'))
                except Exception:
                    pass

        # Otherwise examine formats list
        fmts = info.get('formats') or []
        # Prefer formats that contain both audio and video (non-dash progressive)
        candidates = []
        for f in fmts:
            vcodec = f.get('vcodec')
            acodec = f.get('acodec')
            proto = f.get('protocol') or ''
            url = f.get('url')
            # skip if no url
            if not url:
                continue
            # skip obvious image/thumbnail storyboard urls
            ul = (url or '').lower()
            if ul.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in ul or 'storyboard' in ul or 'googleusercontent' in ul:
                try:
                    self.event_q.put(('log', f'_resolve_stream_url: skipping image-like format.url: {url[:160]}'))
                except Exception:
                    pass
                continue
            # Accept HLS/m3u8 or plain http(s) progressive streams, but avoid DASH/MSE fragments
            proto_l = proto.lower()
            if proto_l.startswith('dash') or 'mse' in proto_l:
                # skip DASH/MSE fragments which libVLC sometimes cannot demux or requires special handling
                continue
            # Accept HLS (m3u8) or http(s) progressive streams
            if proto_l.startswith('m3u8') or proto_l.startswith('http') or proto_l.startswith('https'):
                # prefer formats with both codecs
                score = 0
                if vcodec and vcodec != 'none':
                    score += 2
                if acodec and acodec != 'none':
                    score += 2
                # prefer mp4 ext
                if f.get('ext') == 'mp4':
                    score += 1
                candidates.append((score, f))

        # If no candidates found (e.g., only DASH available), fall back to allowing DASH
        if not candidates:
            for f in fmts:
                url = f.get('url')
                if not url:
                    continue
                vcodec = f.get('vcodec')
                acodec = f.get('acodec')
                score = 0
                if vcodec and vcodec != 'none':
                    score += 2
                if acodec and acodec != 'none':
                    score += 2
                if f.get('ext') == 'mp4':
                    score += 1
                candidates.append((score, f))

        if not candidates:
            return None

        # choose best scored candidate; log its protocol/ext for debugging
        candidates.sort(key=lambda x: x[0], reverse=True)
        best = candidates[0][1]
        # If the chosen candidate somehow points to an image, skip to next valid candidate
        chosen = None
        for score, f in candidates:
            u = f.get('url')
            ul = (u or '').lower()
            if ul.endswith(('.jpg', '.jpeg', '.png', '.webp')) or 'ytimg' in ul or 'storyboard' in ul or 'googleusercontent' in ul:
                try:
                    self.event_q.put(('log', f'_resolve_stream_url: candidate url looks like image, skipping: {u[:160]}'))
                except Exception:
                    pass
                continue
            chosen = f
            break
        if chosen is None:
            try:
                self.event_q.put(('log', '_resolve_stream_url: no non-image candidates found'))
            except Exception:
                pass
            return None
        try:
            self.event_q.put(('log', f"_resolve_stream_url: chosen protocol={chosen.get('protocol')} ext={chosen.get('ext')} url={'(trimmed)' if chosen.get('url') and len(chosen.get('url'))>200 else chosen.get('url')}"))
        except Exception:
            pass
        return chosen.get('url')

    def _fetch_thumbnail(self, item, iid):
        try:
            if not getattr(item, 'thumbnail', None):
                return
            url = item.thumbnail
            import urllib.request, io
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = resp.read()
            img = None
            if Image is not None and ImageTk is not None:
                bio = io.BytesIO(data)
                pil = Image.open(bio)
                pil.thumbnail((320, 180))
                img = ImageTk.PhotoImage(pil)
            if img:
                item.thumbnail_image = img
                # update UI on main thread
                def do_set():
                    sel = self.tree.selection()
                    if sel and sel[0] == iid:
                        try:
                            self.thumb_label.config(image=img)
                            self.thumb_label.image = img
                        except Exception:
                            self.thumb_label.config(text=(item.title or item.url))
                self.event_q.put(('show_dialog', do_set))
        except Exception as e:
            self.event_q.put(('log', f'Failed to fetch thumbnail: {e}'))

    def _search_and_add(self, query: str, max_results: int = 20):
        # perform a youtube search and show selection dialog
        self.event_q.put(('log', f"Searching YouTube for: {query}"))
        qstr = f"ytsearch{max_results}:{query}"
        results = None
        try:
            info = self._fetch_info(qstr)
            results = info.get('entries', []) if info else []
        except Exception as e:
            self.event_q.put(('log', f"Search failed: {e}"))
            results = []

        # present dialog to user in main thread
        def show_results():
            dlg = tk.Toplevel(self)
            dlg.title('Search results')
            # track this dialog so preview can release its grab if needed
            try:
                self._last_search_dialog = dlg
                dlg.bind('<Destroy>', lambda e: setattr(self, '_last_search_dialog', None))
            except Exception:
                pass
            # Size the dialog to fit the screen while preserving a sensible maximum
            try:
                sw = self.winfo_screenwidth()
                sh = self.winfo_screenheight()
                w = min(1280, max(800, sw - 40))
                h = min(800, max(480, sh - 80))
                x = max(0, (sw - w) // 2)
                y = max(0, (sh - h) // 2)
                dlg.geometry(f"{w}x{h}+{x}+{y}")
                dlg.minsize(600, 360)
            except Exception:
                dlg.geometry('1280x800')
            dlg.transient(self)
            dlg.grab_set()

            # Log fetch
            self.event_q.put(('log', f"Search fetched {len(results)} entries for query: '{query}'"))

            # top header: title + count + refine
            header = ttk.Frame(dlg)
            header.pack(fill='x', padx=12, pady=(10, 6))
            title_lbl = ttk.Label(header, text=f"Search results for: {query}", font=('Segoe UI', 12, 'bold'))
            title_lbl.pack(side='left')
            count_lbl = ttk.Label(header, text=f"{len(results)} results", foreground='#666')
            count_lbl.pack(side='left', padx=(10,0))

            # refine box
            refine_var = tk.StringVar()
            ref_frame = ttk.Frame(header)
            ref_frame.pack(side='right')
            ttk.Label(ref_frame, text='Refine:').pack(side='left', padx=(0,6))
            refine_entry = ttk.Entry(ref_frame, textvariable=refine_var, width=32)
            refine_entry.pack(side='left')
            ttk.Button(ref_frame, text='Clear', command=lambda: refine_var.set('')).pack(side='left', padx=(6,0))

            # Canvas + scrollbar to host grid of cards (better for many results)
            container = ttk.Frame(dlg)
            container.pack(fill='both', expand=True, padx=12, pady=8)

            canvas = tk.Canvas(container, highlightthickness=0)
            vscroll = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)
            canvas.configure(yscrollcommand=vscroll.set)
            vscroll.pack(side='right', fill='y')
            canvas.pack(side='left', fill='both', expand=True)

            inner = ttk.Frame(canvas)
            # create window inside canvas
            canvas.create_window((0,0), window=inner, anchor='nw')

            # cards list
            cards = []
            seen_ids = set()

            PAGE_SIZE = 10
            total_results = len(results)
            total_pages = max(1, (total_results + PAGE_SIZE - 1) // PAGE_SIZE)
            current_page = {'n': 1}

            def on_config(event=None):
                canvas.configure(scrollregion=canvas.bbox('all'))
            inner.bind('<Configure>', on_config)

            def format_duration(sec):
                try:
                    s = int(sec)
                    m, s = divmod(s, 60)
                    h, m = divmod(m, 60)
                    if h:
                        return f"{h:d}:{m:02d}:{s:02d}"
                    return f"{m:d}:{s:02d}"
                except Exception:
                    return ''

            def make_card(parent, entry):
                # Use ttk for consistent styling
                card = ttk.Frame(parent, style='Card.TFrame')
                card.config(width=220)
                # internal layout: thumbnail on top, metadata, actions
                thumb_lbl = ttk.Label(card, text='', anchor='center')
                thumb_lbl.pack(fill='x', pady=(8,4))
                title = entry.get('title') or entry.get('id')
                ttl = ttk.Label(card, text=title, wraplength=220, justify='center', style='CardTitle.TLabel')
                ttl.pack(padx=8)
                uploader = entry.get('uploader') or ''
                dur = format_duration(entry.get('duration'))
                # views and publish date formatting (if available)
                views_txt = format_views(entry.get('view_count') or entry.get('viewCount') or entry.get('average_rating'))
                pub_txt = format_publish(entry)
                parts = []
                if uploader:
                    parts.append(uploader)
                if views_txt:
                    parts.append(views_txt)
                if pub_txt:
                    parts.append(pub_txt)
                if dur:
                    parts.append(dur)
                meta_text = '  •  '.join(parts) if parts else ''
                meta = ttk.Label(card, text=meta_text, style='CardMeta.TLabel')
                meta.pack(padx=8, pady=(2,8))

                # action row
                btn_row = ttk.Frame(card)
                btn_row.pack(fill='x', padx=8, pady=(0,8))
                add_single_btn = ttk.Button(btn_row, text='Add', width=10)
                add_single_btn.pack(side='left')
                preview_btn = ttk.Button(btn_row, text='Preview', width=10, command=lambda e=entry: self._open_preview_for_entry(e))
                preview_btn.pack(side='right')

                # hover effect
                def on_enter(e):
                    try:
                        card.state(['active'])
                    except Exception:
                        pass
                def on_leave(e):
                    try:
                        card.state(['!active'])
                    except Exception:
                        pass
                card.bind('<Enter>', on_enter)
                card.bind('<Leave>', on_leave)

                # add action implementation
                def add_single():
                    url = entry.get('webpage_url') or f"https://www.youtube.com/watch?v={entry.get('id')}"
                    it = DownloadItem(url, fmt=self.format_var.get(), bitrate=self.bitrate_var.get())
                    it.title = entry.get('title')
                    it.thumbnail = entry.get('thumbnail')
                    self.queue_items.append(it)
                    iid = str(id(it))
                    self.iid_map[iid] = it
                    self.tree.insert('', 'end', iid=iid, values=(it.title or it.url, it.status, self._format_progress_bar(it.progress)))
                    self.event_q.put(('log', f"Added item from search: {it.title or it.url}"))
                    try:
                        self._draw_progress_overlays()
                    except Exception:
                        pass

                add_single_btn.config(command=add_single)

                # async thumbnail fetch
                def fetch_thumb():
                    url = entry.get('thumbnail')
                    if not url:
                        return
                    try:
                        import urllib.request, io
                        with urllib.request.urlopen(url, timeout=8) as resp:
                            data = resp.read()
                        if Image is not None and ImageTk is not None:
                            bio = io.BytesIO(data)
                            pil = Image.open(bio)
                            pil.thumbnail((220,130))
                            img = ImageTk.PhotoImage(pil)
                            def put():
                                try:
                                    thumb_lbl.config(image=img, text='')
                                    thumb_lbl.image = img
                                except Exception:
                                    pass
                            self.event_q.put(('show_dialog', put))
                    except Exception:
                        pass

                threading.Thread(target=fetch_thumb, daemon=True).start()

                return card

            # Helpers: format views and publish date
            def format_views(v):
                try:
                    if not v:
                        return ''
                    return f"{int(v):,} views"
                except Exception:
                    return ''

            def format_publish(entry):
                # try common keys: upload_date (YYYYMMDD) or timestamp
                try:
                    d = entry.get('upload_date') or entry.get('release_date')
                    if d and isinstance(d, str) and len(d) == 8:
                        return f"Published: {d[0:4]}-{d[4:6]}-{d[6:8]}"
                    ts = entry.get('timestamp') or entry.get('release_timestamp')
                    if ts:
                        import datetime
                        dt = datetime.datetime.fromtimestamp(int(ts))
                        return f"Published: {dt.date().isoformat()}"
                except Exception:
                    pass
                return ''

            # Create all cards but don't grid all at once — render per page
            for e in results:
                vid = e.get('id')
                if vid:
                    seen_ids.add(vid)
                cards.append((e, make_card(inner, e)))

            def layout_page(page_num:int):
                # clear grid
                for _, c in cards:
                    c.grid_forget()
                start = (page_num-1)*PAGE_SIZE
                end = start + PAGE_SIZE
                visible = cards[start:end]
                cols = 5
                for i, (entry, card) in enumerate(visible):
                    r = i // cols
                    c = i % cols
                    card.grid(row=r, column=c, padx=10, pady=10, sticky='n')
                # update count label
                page_lbl.config(text=f"Page {page_num} / {total_pages}")
                count_lbl.config(text=f"{len(results)} results")

            # controls
            ctrl = ttk.Frame(dlg)
            ctrl.pack(fill='x', padx=12, pady=(6,12))
            add_selected_btn = ttk.Button(ctrl, text='Add Selected', command=lambda: None)
            # selection model: simple - we don't keep multi-select cards in this version; use per-card Add
            add_selected_btn.state(['disabled'])
            add_selected_btn.pack(side='left')

            # loading indicator (hidden until needed)
            load_progress = ttk.Progressbar(ctrl, mode='indeterminate', length=180)
            load_progress.pack(side='left', padx=(6,4))
            load_progress.pack_forget()

            load_more_btn = ttk.Button(ctrl, text='Load more')
            load_more_btn.pack(side='left', padx=6)
            prev_btn = ttk.Button(ctrl, text='◀ Prev', command=lambda: (current_page.update(n=max(1, current_page['n']-1)) or layout_page(current_page['n'])))
            prev_btn.pack(side='left', padx=6)
            page_lbl = ttk.Label(ctrl, text=f"Page {current_page['n']} / {total_pages}")
            page_lbl.pack(side='left')
            next_btn = ttk.Button(ctrl, text='Next ▶', command=lambda: (current_page.update(n=min(total_pages, current_page['n']+1)) or layout_page(current_page['n'])))
            next_btn.pack(side='left', padx=6)
            ttk.Button(ctrl, text='Cancel', command=dlg.destroy).pack(side='right')

            def load_more_background():
                nonlocal results, total_results, total_pages
                try:
                    self.event_q.put(('log', 'Load more requested'))
                    current = len(results)
                    target = current + PAGE_SIZE
                    info2 = self._fetch_info(f"ytsearch{target}:{query}")
                    more = info2.get('entries', []) if info2 else []
                    # pick non-duplicate entries compared to seen_ids
                    new_entries = []
                    for e in more:
                        vid = e.get('id')
                        if not vid:
                            continue
                        if vid in seen_ids:
                            continue
                        seen_ids.add(vid)
                        new_entries.append(e)
                        if len(new_entries) >= PAGE_SIZE:
                            break

                    def add_new_ui():
                        nonlocal results, total_results, total_pages
                        # append to results and create cards for new entries
                        results = results + new_entries
                        added = 0
                        for e in new_entries:
                            c = make_card(inner, e)
                            cards.append((e, c))
                            added += 1
                        total_results = len(results)
                        total_pages = max(1, (total_results + PAGE_SIZE - 1) // PAGE_SIZE)
                        # refresh layout on current page
                        layout_page(current_page['n'])
                        self.event_q.put(('log', f'Load more completed - {added} new cards added'))

                    # schedule UI update
                    self.event_q.put(('show_dialog', add_new_ui))
                except Exception as e:
                    self.event_q.put(('log', f'Load more failed: {e}'))
                finally:
                    # stop progress bar on UI thread
                    def stop_progress_ui():
                        try:
                            load_progress.stop()
                            load_progress.pack_forget()
                            load_more_btn.state(['!disabled'])
                        except Exception:
                            pass
                    self.event_q.put(('show_dialog', stop_progress_ui))

            def on_load_more():
                # disable button, show progress, and run fetch in background
                try:
                    load_more_btn.state(['disabled'])
                    load_progress.pack(side='left', padx=(6,4))
                    load_progress.start(10)
                except Exception:
                    pass
                threading.Thread(target=load_more_background, daemon=True).start()

            load_more_btn.config(command=on_load_more)

            # refine filter
            def apply_refine(*a):
                q = refine_var.get().strip().lower()
                if not q:
                    # restore original page
                    layout_page(1)
                    current_page['n'] = 1
                    return
                filtered = [(e,c) for (e,c) in cards if q in ((e.get('title') or '') + ' ' + (e.get('uploader') or '')).lower()]
                # temporarily show first PAGE_SIZE matches
                for _, c in cards:
                    c.grid_forget()
                for i, (e,c) in enumerate(filtered[:PAGE_SIZE]):
                    r = i // 5
                    col = i % 5
                    c.grid(row=r, column=col, padx=10, pady=10, sticky='n')
                page_lbl.config(text=f"Filter: {len(filtered)} shown")
                self.event_q.put(('log', f"Refine applied: '{q}' – {len(filtered)} matches"))

            refine_var.trace_add('write', apply_refine)

            # initial render
            dlg.after(80, lambda: layout_page(1))
            # ensure the dialog has a local grab so main app is inactive while showing
            try:
                dlg.grab_set()
            except Exception:
                pass

        self.event_q.put(('show_dialog', show_results))

    def _set_item_status(self, idx, status):
        # idx may be numeric index or iid
        try:
            if isinstance(idx, str):
                iid = idx
                item = self.iid_map.get(iid)
                if item:
                    item.status = status
                    try:
                        self.tree.set(iid, 'status', status)
                    except Exception:
                        pass
            else:
                if idx < len(self.queue_items):
                    item = self.queue_items[idx]
                    item.status = status
                    # if item has iid, update tree
                    iid = None
                    for k, v in list(self.iid_map.items()):
                        if v is item:
                            iid = k
                            break
                    if iid:
                        try:
                            self.tree.set(iid, 'status', status)
                        except Exception:
                            pass
        except Exception:
            pass

    def _download_item(self, idx, item: DownloadItem):
        # Prepare output template
        outdir = Path(self.output_dir.get())
        outdir.mkdir(parents=True, exist_ok=True)
        safe_title = '%(title)s'
        outtmpl = str(outdir / (safe_title + '.%(ext)s'))

        if item.fmt == 'mp3':
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': outtmpl,
                'noplaylist': True,
                'quiet': True,
                'no_warnings': True,
                'progress_hooks': [self._make_progress_hook(idx, item)],
            }
        else:
            ydl_opts = {
                'format': 'bestvideo+bestaudio/best',
                'outtmpl': outtmpl,
                'noplaylist': True,
                'quiet': True,
                'no_warnings': True,
                'progress_hooks': [self._make_progress_hook(idx, item)],
                # Remove any audio-only postprocessors for mp4
            }

        # If user provided an ffmpeg location (full path or directory), tell yt-dlp where to find it
        try:
            if getattr(self, 'ffmpeg_path', None):
                # yt-dlp expects the ffmpeg location (directory containing ffmpeg binaries)
                ff = self.ffmpeg_path
                ff_dir = ff if os.path.isdir(ff) else os.path.dirname(ff)
                if ff_dir:
                    ydl_opts['ffmpeg_location'] = ff_dir
        except Exception:
            pass

        if item.fmt == 'mp3':
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': str(item.bitrate),
            }]
        else:
            # ensure merged to mp4
            ydl_opts['merge_output_format'] = 'mp4'
            # if user selected a specific quality (format id), use it
            try:
                q = getattr(item, 'quality', None)
                if q and q != 'auto':
                    # use the selected yt-dlp format id; prefer combining video+audio where needed
                    ydl_opts['format'] = q
            except Exception:
                pass

        # Run ydl
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(item.url, download=True)
                # after download, determine filename
                filename = ydl.prepare_filename(info)
                # for mp3, replace ext
                if item.fmt == 'mp3':
                    filename = os.path.splitext(filename)[0] + '.mp3'
                item.filename = filename
                item.status = 'Completed'
                self.event_q.put(('log', f'Completed: {item.title or item.url} -> {filename}'))
                # update tree entry by iid
                iid = None
                for k, v in list(self.iid_map.items()):
                    if v is item:
                        iid = k
                        break
                if iid:
                    self.event_q.put(('update_item', (iid, (item.title or item.url, 'Completed', '100%'))))
                else:
                    self._set_item_status(idx, 'Completed')
            except Exception as e:
                item.error = str(e)
                item.status = 'Error'
                self.event_q.put(('log', f'Failed: {item.title or item.url} — {e}'))
                iid = None
                for k, v in list(self.iid_map.items()):
                    if v is item:
                        iid = k
                        break
                if iid:
                    self.event_q.put(('update_item', (iid, (item.title or item.url, 'Error', '0%'))))
                else:
                    self._set_item_status(idx, 'Error')

    def _make_progress_hook(self, idx, item):
        def hook(d):
            status = d.get('status')
            if status == 'downloading':
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                downloaded = d.get('downloaded_bytes') or 0
                percent = (downloaded / total * 100) if total else 0.0
                item.progress = percent
                # update by iid if available
                iid = None
                for k, v in list(self.iid_map.items()):
                    if v is item:
                        iid = k
                        break
                if iid:
                    self.event_q.put(('progress', (iid, percent)))
                else:
                    self.event_q.put(('progress', (idx, percent)))
                # also send a log message occasionally
                if int(percent) % 5 == 0:
                    self.event_q.put(('log', f"{int(percent)}% — {item.url}"))
            elif status == 'finished':
                self.event_q.put(('log', f"Converting/finishing: {item.url}"))
        return hook

    def _format_progress_bar(self, percent: float, width: int = 12) -> str:
        try:
            p = max(0, min(100, int(percent)))
        except Exception:
            p = 0
        filled = int(p / 100 * width)
        bar = '█' * filled + '░' * (width - filled)
        return f"{bar} {p:3d}%"

    # --- Overlay canvas for graphical per-row progress bars ---
    def _create_progress_overlay(self, parent):
        # The overlay canvas is placed on top of the Treeview to draw progress bars.
        self._overlay = tk.Canvas(parent, highlightthickness=0, bg='', bd=0)
        # place the canvas over the treeview area
        self._overlay.place(in_=self.tree, relx=0, rely=0, relwidth=1, relheight=1)
        self._overlay_items = {}

        # forward clicks from canvas to tree selection
        self._overlay.bind('<Button-1>', self._on_overlay_click)
        self._overlay.bind('<Double-Button-1>', self._on_overlay_double)
        # forward mouse wheel so scrolling still works when pointer over overlay
        self._overlay.bind('<Enter>', lambda e: self._overlay.focus_set())
        self._overlay.bind('<MouseWheel>', self._on_overlay_mousewheel)
        # linux wheel
        self._overlay.bind('<Button-4>', self._on_overlay_mousewheel)
        self._overlay.bind('<Button-5>', self._on_overlay_mousewheel)

        # redraw when tree resizes or config changes
        self.tree.bind('<Configure>', lambda e: self._draw_progress_overlays())

    def _on_tree_yscroll(self, *args):
        # wrapper for tree yscrollcommand - update scrollbar and redraw overlays
        try:
            if hasattr(self, '_queue_scrollbar') and self._queue_scrollbar:
                self._queue_scrollbar.set(*args)
        except Exception:
            pass
        # schedule a redraw shortly after scroll
        try:
            self.after(10, self._draw_progress_overlays)
        except Exception:
            pass

    def _on_overlay_mousewheel(self, event):
        # forward wheel to tree
        try:
            if event.num == 4:
                # linux up
                self.tree.yview_scroll(-1, 'units')
            elif event.num == 5:
                # linux down
                self.tree.yview_scroll(1, 'units')
            else:
                # windows/macos
                self.tree.yview_scroll(int(-1*(event.delta/120)), 'units')
        except Exception:
            pass

    def _on_overlay_click(self, event):
        # translate overlay click to tree selection
        try:
            y = event.y
            iid = self.tree.identify_row(y)
            if iid:
                self.tree.selection_set(iid)
                # ensure focus and callback
                self.tree.focus(iid)
                self._on_tree_select()
        except Exception:
            pass

    def _on_overlay_double(self, event):
        try:
            y = event.y
            iid = self.tree.identify_row(y)
            if iid:
                # select and open output (double-click behavior)
                self.tree.selection_set(iid)
                self._on_tree_select()
                self.on_open_output()
        except Exception:
            pass

    def _draw_progress_overlays(self):
        # Draw a rounded progress bar in the 'progress' column for visible rows
        try:
            if not hasattr(self, '_overlay'):
                return
            canvas = self._overlay
            canvas.delete('all')
            cols = list(self.tree['columns'])
            # compute x offset for progress column
            x = 0
            col_x = {}
            for c in cols:
                w = int(self.tree.column(c, 'width'))
                col_x[c] = (x, w)
                x += w

            for iid in self.tree.get_children(''):
                bbox = self.tree.bbox(iid)
                if not bbox:
                    continue
                x0, y0, w, h = bbox
                # get item and progress
                item = self.iid_map.get(iid)
                if not item:
                    continue
                pct = getattr(item, 'progress', 0.0) or 0.0
                # compute progress cell rectangle
                if 'progress' in col_x:
                    px, pw = col_x['progress']
                    bar_x0 = px + 6
                    bar_x1 = px + pw - 6
                else:
                    # fallback: right-most area
                    bar_x0 = x - 160
                    bar_x1 = x - 20
                # convert tree-relative x to canvas coordinates (overlay placed in_=tree)
                bar_y0 = y0 + 4
                bar_y1 = y0 + h - 4
                # draw background
                canvas.create_rectangle(bar_x0, bar_y0, bar_x1, bar_y1, fill='#e6e6e6', outline='')
                # filled portion
                try:
                    fill_w = int((pct / 100.0) * (bar_x1 - bar_x0))
                except Exception:
                    fill_w = 0
                if fill_w > 0:
                    canvas.create_rectangle(bar_x0, bar_y0, bar_x0 + fill_w, bar_y1, fill='#4CAF50', outline='')
                # percent text
                canvas.create_text((bar_x0 + bar_x1) / 2, (bar_y0 + bar_y1) / 2, text=f"{int(pct)}%", fill='white' if pct > 30 else 'black', font=('Segoe UI', 9, 'bold'))
        except Exception:
            pass

    def _schedule_poll(self):
        # start polling the event queue
        self.after(200, self._poll_events)

    def _poll_events(self):
        try:
            while True:
                typ, data = self.event_q.get_nowait()
                if typ == 'progress':
                    iid, percent = data
                    bar = self._format_progress_bar(percent)
                    if isinstance(iid, str) and iid in self.iid_map:
                        item = self.iid_map[iid]
                        try:
                            self.tree.set(iid, 'status', item.status)
                            self.tree.set(iid, 'progress', bar)
                        except Exception:
                            pass
                        try:
                            self._draw_progress_overlays()
                        except Exception:
                            pass
                    else:
                        # fallback to idx-based progress
                        try:
                            idx, percent = data
                            if 0 <= idx < len(self.queue_items):
                                item = self.queue_items[idx]
                                # update by iid if exists
                                iid = None
                                for k, v in list(self.iid_map.items()):
                                    if v is item:
                                        iid = k
                                        break
                                if iid:
                                    try:
                                        bar = self._format_progress_bar(percent)
                                        self.tree.set(iid, 'status', item.status)
                                        self.tree.set(iid, 'progress', bar)
                                    except Exception:
                                        pass
                                    try:
                                        self._draw_progress_overlays()
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                elif typ == 'log':
                    self.log(data)
                elif typ == 'update_item':
                    key, payload = data
                    # payload may be a tuple (title,status,progress) or a simple string
                    if isinstance(key, str) and key in self.iid_map:
                        iid = key
                        item = self.iid_map.get(iid)
                        if isinstance(payload, tuple) and len(payload) == 3:
                            title, status, progress = payload
                            try:
                                self.tree.set(iid, 'title', title)
                                self.tree.set(iid, 'status', status)
                                # normalize progress value to a bar when possible
                                prog_val = progress
                                try:
                                    if isinstance(progress, str) and progress.strip().endswith('%'):
                                        pct = int(progress.strip().rstrip('%'))
                                        prog_val = self._format_progress_bar(pct)
                                    elif isinstance(progress, (int, float)):
                                        prog_val = self._format_progress_bar(float(progress))
                                except Exception:
                                    pass
                                self.tree.set(iid, 'progress', prog_val)
                                # if this row is selected, update preview title
                                try:
                                    sel = self.tree.selection()
                                    if sel and sel[0] == iid:
                                        self.meta_title.set(f"Title: {title}")
                                except Exception:
                                    pass
                            except Exception:
                                pass
                            try:
                                                                                                                             self._draw_progress_overlays()
                            except Exception:
                                pass
                        else:
                            try:
                                self.tree.set(iid, 'title', str(payload))
                                try:
                                    sel = self.tree.selection()
                                    if sel and sel[0] == iid:
                                        self.meta_title.set(str(payload))
                                except Exception:
                                    pass
                            except Exception:
                                pass
                    else:
                        # fallback: key is index
                        try:
                            idx = int(key)
                            if 0 <= idx < len(self.queue_items):
                                item = self.queue_items[idx]
                                text = payload if isinstance(payload, str) else f"{item.title or item.url} — {item.status}"
                                # replace the tree entry if it exists
                                # find iid
                                iid = None
                                for k, v in list(self.iid_map.items()):
                                    if v is item:
                                        iid = k
                                        break
                                if iid:
                                    try:
                                        self.tree.set(iid, 'title', item.title or item.url)
                                        self.tree.set(iid, 'status', item.status)
                                        self.tree.set(iid, 'progress', self._format_progress_bar(item.progress))
                                        try:
                                            sel = self.tree.selection()
                                            if sel and sel[0] == iid:
                                                self.meta_title.set(f"Title: {item.title or item.url}")
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass
                                    try:
                                        self._draw_progress_overlays()
                                    except Exception:
                                        pass
                                else:
                                    # insert new row
                                    iid_new = str(id(item))
                                    self.iid_map[iid_new] = item
                                    try:
                                        self.tree.insert('', idx, iid=iid_new, values=(item.title or item.url, item.status, self._format_progress_bar(item.progress)))
                                        try:
                                            sel = self.tree.selection()
                                            if sel and sel[0] == iid_new:
                                                self.meta_title.set(f"Title: {item.title or item.url}")
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass
                                    try:
                                        self._draw_progress_overlays()
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                elif typ == 'show_dialog':
                    # data is a callable that will create a dialog or update UI
                    try:
                        data()
                    except Exception as e:
                        self.log('Failed to show dialog:', e)
                elif typ == 'status':
                    self.status_var.set(data)
                elif typ == 'done':
                    self.status_var.set('Idle')
                    self.log('All downloads finished or stopped')
        except queue.Empty:
            pass
        self.after(200, self._poll_events)


def main():
    app = App()
    app.mainloop()


if __name__ == '__main__':
    main()
