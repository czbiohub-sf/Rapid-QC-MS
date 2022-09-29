import sys
import logging
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
import sqlalchemy as db

class DataAcquisitionEventHandler(FileSystemEventHandler):

    """
    Event handler that alerts when the data file has completed sample acquisition
    """

    def __init__(self, observer, instrument_id, run_id, filenames):

        self.observer = observer
        self.instrument_id = instrument_id
        self.run_id = run_id
        self.filenames = filenames


    def on_modified(self, event):

        """
        Listen for data file creation
        """

        # Remove directory path and file extension from filename
        filename = event.src_path.split("/")[-1].split(".")[0]

        # Check if file created is in the sequence
        if not event.is_directory and filename in self.filenames:

            # Get MD5 checksum and write to database to detect when file is done acquiring
            md5_checksum = get_md5(event.src_path)

            # TODO: Execute QC processing
            print("Acquisition completed")

            # Terminate listener when the last data file is acquired
            if filename == self.filenames[-1]:
                self.observer.stop()


    def watch_file(self, filename, check_interval=300):

        """
        Returns True if MD5 checksum on file matches the MD5 checksum written to the database 5 minutes ago.
        Effectively determines whether sample acquisition has been completed.
        """

        # TODO: Update this function
        now = time.time()

        while time.time() <= last_time:
            if os.path.exists(filename):
                return True
            else:
                # Wait for check interval seconds, then check again.
                time.sleep(check_interval)

        return False


def start_listener(path, filenames):

    """
    Watchdog file monitor to get files in directory upon data acquisition completion
    """

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    observer = Observer()
    event_handler = DataAcquisitionEventHandler(observer, filenames)
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()


def get_md5(filename):

    """
    Returns MD5 checksum of a file
    """

    hash_md5 = hashlib.md5()

    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()