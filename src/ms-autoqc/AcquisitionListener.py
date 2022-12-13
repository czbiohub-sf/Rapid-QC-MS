import os, sys, time
import logging
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
import DatabaseFunctions as db
import AutoQCProcessing as qc

class DataAcquisitionEventHandler(FileSystemEventHandler):

    """
    Event handler that alerts when the data file has completed sample acquisition
    """

    def __init__(self, observer, filenames, run_id):

        self.observer = observer
        self.filenames = filenames
        self.run_id = run_id


    def on_created(self, event):

        """
        Listen for data file creation
        """

        # Remove directory path and file extension from filename
        path = event.src_path.replace("\\", "/")
        filename = path.split("/")[-1].split(".")[0]
        extension = path.split("/")[-1].split(".")[-1]
        path = path.replace(filename + "." + extension, "")

        # Check if file created is in the sequence
        if not event.is_directory and filename in self.filenames:

            print("File created:", filename)
            print("Watching file...")

            # Start watching file until sample acquisition is complete
            sample_acquired = self.watch_file(path, filename, extension)

            # Execute QC processing
            if sample_acquired:
                print("Data acquisition completed for", filename)
                qc.process_data_file(event.src_path, filename, extension, self.run_id)
                print("Data processing complete.")

            # Terminate listener when the last data file is acquired
            if filename == self.filenames[-1]:
                print("Last sample acquired. Instrument run complete.")
                self.observer.stop()
                pid = db.get_pid(self.run_id)
                qc.kill_acquisition_listener(pid)
                print("Terminated acquisition listener process.")


    def watch_file(self, path, filename, extension, check_interval=180):

        """
        Returns True if MD5 checksum on file matches the MD5 checksum written to the database 3 minutes ago.
        Effectively determines whether sample acquisition has been completed.
        """

        # Write initial MD5 checksum to database
        md5_checksum = get_md5(path + filename + "." + extension)
        db.update_md5_checksum(filename, md5_checksum)

        # Watch file indefinitely
        while os.path.exists(path):

            print("MD5 checksums do not match. Waiting 3 minutes...")

            # Wait 5 minutes
            time.sleep(check_interval)

            new_md5 = get_md5(path + filename + "." + extension)
            old_md5 = db.get_md5(filename)

            print("Comparing MD5 checksums...")

            # If the MD5 checksum after 3 mins is the same as before, file is done acquiring
            if new_md5 == old_md5:
                return True
                break
            else:
                db.update_md5_checksum(filename, new_md5)


def start_listener(path, filenames, run_id):

    """
    Watchdog file monitor to get files in directory upon data acquisition completion
    """

    print("Run monitoring initiated for", path)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    observer = Observer()
    event_handler = DataAcquisitionEventHandler(observer, filenames, run_id)
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
    start_listener(path=sys.argv[1], filenames=sys.argv[2], run_id=sys.argv[3])