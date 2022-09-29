import os, sys, time
import logging
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
import DatabaseFunctions as db

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

        print(event)
        print(self.filenames)

        # Remove directory path and file extension from filename
        filename = event.src_path.split("/")[-1].split(".")[0]

        # Check if file created is in the sequence
        if not event.is_directory and filename in self.filenames:

            print("File created:", filename)
            print("Watching file...")

            # Start watching file until sample acquisition is complete
            sample_acquired = self.watch_file(event.src_path, filename)

            # TODO: Execute QC processing
            if sample_acquired:
                print("Data acquisition completed for", filename)

            # Terminate listener when the last data file is acquired
            if filename == self.filenames[-1]:
                print("Last sample acquired. Instrument run complete.")
                self.observer.stop()


    def watch_file(self, path, filename, check_interval=180):

        """
        Returns True if MD5 checksum on file matches the MD5 checksum written to the database 3 minutes ago.
        Effectively determines whether sample acquisition has been completed.
        """

        # Write initial MD5 checksum to database
        md5_checksum = get_md5(path)
        db.update_md5_checksum(filename, md5_checksum)

        # Watch file indefinitely
        while os.path.exists(path):

            print("MD5 checksums do not match. Waiting 3 minutes...")

            # Wait 5 minutes
            time.sleep(check_interval)

            new_md5 = get_md5(path)
            old_md5 = db.get_md5(filename)

            print("Comparing MD5 checksums...")

            # If the MD5 checksum after 3 mins is the same as before, file is done acquiring
            if new_md5 == old_md5:
                return True
            else:
                db.update_md5_checksum(filename, new_md5)


def start_listener(path, filenames):

    """
    Watchdog file monitor to get files in directory upon data acquisition completion
    """

    print("Run monitoring initiated for", path)

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


if __name__ == "__main__":
    # Start listening to data file directory
    start_listener(path=sys.argv[1], filenames=sys.argv[2])