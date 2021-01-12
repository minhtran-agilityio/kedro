import os
import subprocess
import yaml

from typing import Any

from kedro.io import AbstractDataSet


class YouTubeDataSet(AbstractDataSet):

    def __init__(self, filepath):
        self._filepath = filepath
        self._data_dir = filepath + "_data"

    def _save(self, url: str) -> None:
        os.makedirs(self._data_dir, exist_ok=True)

        def ydl(*args):
            filename_format = "%(id)s.%(ext)s"
            return subprocess.check_output(
                ["youtube-dl"] + list(args) + ["-o", filename_format, url],
                cwd=self._data_dir,
            ).decode("utf8").strip()

        subs_list = ydl("--list-subs")
        has_subs = "has no subtitles" not in subs_list

        title = ydl("--get-title")
        filename = ydl("--get-filename")
        vid = ydl("--get-id")

        description_path = os.path.join(self._data_dir, f"{vid}.description")

        if os.path.exists(description_path):
            os.remove(description_path)

        if has_subs:
            ydl("--write-sub", "--write-description")
        else:
            ydl("--write-auto-sub", "--write-description")

        with open(description_path, encoding="utf8") as f:
            description = f.read()

        with open(self._filepath, "+w", encoding="utf8") as f:
            yaml.dump({
                "id": vid,
                "title": title,
                "filename": filename,
                "filepath": os.path.join(self._data_dir, filename),
                "description": description
            }, f)

    def _load(self) -> Any:
        with open(self._filepath) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def _describe(self):
        return dict(filepath=self._filepath)
