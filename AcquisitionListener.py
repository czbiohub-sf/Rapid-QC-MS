import sys
import logging
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class DataAcquisitionEventHandler(FileSystemEventHandler):

    """
    Event handler that alerts when the data file has completed sample acquisition
    """

    def __init__(self, observer, filenames):
        self.observer = observer
        self.filenames = filenames

    def on_created(self, event):

        """
        Listen for data file creation
        """

        # Remove directory path and file extension from filename
        filename = event.src_path.split("/")[-1].split(".")[0]

        # Check if file created is in the sequence
        if not event.is_directory and filename in self.filenames:

            # TODO: Execute QC processing
            print("Acquisition completed")

            # Terminate listener when the last data file is acquired
            if filename == self.filenames[-1]:
                self.observer.stop()


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