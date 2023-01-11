import os, sys, time, ast, shutil
import logging
import traceback
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

    def __init__(self, observer, filenames, instrument_id, run_id):

        self.observer = observer
        self.filenames = filenames
        self.instrument_id = instrument_id
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
            try:
                sample_acquired = self.watch_file(path, filename, extension)
            except Exception as error:
                print("Unable to watch file:", error)
                sample_acquired = None

            # Route data file to MS-AutoQC pipeline
            if sample_acquired:
                print("Data acquisition completed for", filename)
                qc.process_data_file(path, filename, extension, self.instrument_id, self.run_id)
                print("Data processing complete.")

            # Check if data file was the last sample in the sequence
            if filename == self.filenames[-1]:

                # If so, stop acquisition listening
                print("Last sample acquired. Instrument run complete.")
                self.observer.stop()

                # Terminate acquisition listener process
                print("Terminating acquisition listener process.")
                terminate_job(self.instrument_id, self.run_id)


    def watch_file(self, path, filename, extension):

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

            # Wait 3 minutes
            time.sleep(180)

            new_md5 = get_md5(path + filename + "." + extension)
            old_md5 = db.get_md5(filename)

            print("Comparing MD5 checksums...")

            # If the MD5 checksum after 3 mins is the same as before, file is done acquiring
            if new_md5 == old_md5:
                time.sleep(180)
                return True
            else:
                db.update_md5_checksum(filename, new_md5)


def start_listener(path, filenames, instrument_id, run_id, is_completed_run):

    """
    Watchdog file monitor to get files in directory upon data acquisition completion
    """

    print("Run monitoring initiated for", path)

    is_completed_run = True if is_completed_run == "True" else False
    filenames = ast.literal_eval(filenames)

    if is_completed_run:

        # Get data file extension
        extension = db.get_data_file_type(instrument_id)
        path = path.replace("\\", "/")
        path = path + "/" if path[-1] != "/" else path

        # Iterate through files and process each one
        for filename in filenames:

            # If file is not in directory, skip it
            full_path = path + filename + "." + extension
            if not os.path.exists(full_path):
                continue

            # Process data file
            qc.process_data_file(path, filename, extension, instrument_id, run_id)
            print("Data processing for", filename, "complete.")

        terminate_job(instrument_id, run_id)

    else:
        # Start file monitor and process files as they are created
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        observer = Observer()
        event_handler = DataAcquisitionEventHandler(observer, filenames, instrument_id, run_id)
        observer.schedule(event_handler, path, recursive=True)
        observer.start()

        try:
            while observer.is_alive():
                observer.join(1)
        finally:
            observer.stop()
            observer.join()


def terminate_job(instrument_id, run_id):

    """
    Wraps up job after the last data file has been routed to the pipeline
    """

    # Mark instrument run as completed
    db.mark_run_as_completed(instrument_id, run_id)

    # Sync database on run completion
    if db.sync_is_enabled():
        db.sync_on_run_completion(instrument_id, run_id)

    # Delete temporary data file directory
    try:
        id = instrument_id.replace(" ", "_") + "_" + run_id
        temp_directory = os.path.join(os.getcwd(), "data", id)
        shutil.rmtree(temp_directory)
    except:
        print("Could not delete temporary data directory.")
        traceback.print_exc()

    # Kill acquisition listener
    pid = db.get_pid(instrument_id, run_id)
    qc.kill_acquisition_listener(pid)


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
    start_listener(path=sys.argv[1], filenames=sys.argv[2], instrument_id=sys.argv[3], run_id=sys.argv[4], is_completed_run=sys.argv[5])