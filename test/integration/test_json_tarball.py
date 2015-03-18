import datetime
import hashlib
import json
import mock
import os
import shutil
import tarfile
import unittest

import notification_utils
import notigen

from shoebox import roll_manager


WORKING_DIR = "test_temp/events"
ARCHIVE_DIR = "test_temp/archive"


class TestWritingJSONRollManager(unittest.TestCase):
    def setUp(self):
        for d in [WORKING_DIR, ARCHIVE_DIR]:
            shutil.rmtree(d, ignore_errors=True)
        os.mkdir(WORKING_DIR)
        os.mkdir(ARCHIVE_DIR)

    def _get_files(self, directory):
        files = []
        for f in os.listdir(directory):
            full = os.path.join(directory, f)
            if os.path.isfile(full):
                files.append((full, os.path.getsize(full)))
        return files

    def test_size_rolling(self):
        manager = roll_manager.WritingJSONRollManager(
                                            "test_[[CRC]].events",
                                            directory=WORKING_DIR,
                                            destination_directory=ARCHIVE_DIR,
                                            roll_size_mb=20)

        g = notigen.EventGenerator("test/integration/templates")
        entries = {}
        now = datetime.datetime.utcnow()
        while len(entries) < 10000:
            events = g.generate(now)
            if events:
                for event in events:
                    json_event = json.dumps(event,
                                        cls=notification_utils.DateTimeEncoder)
                    manager.write({}, json_event)
                    crc = hashlib.sha256(json_event).hexdigest()
                    entries[crc] = json_event

            now = g.move_to_next_tick(now)
        manager.close()

        files = self._get_files(WORKING_DIR)

        archives = self._get_files(ARCHIVE_DIR)
        archived_files = []
        for filename, size in archives:
            tar = tarfile.open(filename, "r:gz")
            for tarinfo in tar:
                if tarinfo.isreg():
                    archived_files.append(tarinfo.name)
            tar.close()

        self.assertEqual(len(files) + len(archived_files), len(events))

