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

    def __init__(self, observer, path, filenames, extension, instrument_id, run_id, current_sample):

        self.observer = observer
        self.path = path
        self.filenames = filenames
        self.extension = extension
        self.instrument_id = instrument_id
        self.run_id = run_id
        self.current_sample = current_sample


    def on_created(self, event):

        """
        Listen for data file creation
        """

        # Remove directory path and file extension from filename
        path = event.src_path.replace("\\", "/")
        filename = path.split("/")[-1].split(".")[0]

        # For restarted jobs: process the sample that was being acquired when the job was interrupted
        if os.path.exists(self.path + self.current_sample + "." + self.extension):
            self.trigger_pipeline(self.path, self.current_sample, self.extension)

        # Route data file to pipeline
        if not event.is_directory and filename in self.filenames:
            self.trigger_pipeline(self.path, filename, self.extension)


    def watch_file(self, path, filename, extension):

        """
        Returns True if MD5 checksum on file matches the MD5 checksum written to the database 3 minutes ago.
        Effectively determines whether sample acquisition has been completed.
        """

        # Write initial MD5 checksum to database
        md5_checksum = get_md5(path + filename + "." + extension)
        db.update_md5_checksum(self.instrument_id, filename, md5_checksum)

        # Watch file indefinitely
        while os.path.exists(path):

            print("MD5 checksums do not match. Waiting 3 minutes...")

            # Wait 3 minutes
            time.sleep(180)

            new_md5 = get_md5(path + filename + "." + extension)
            old_md5 = db.get_md5(self.instrument_id, filename)

            print("Comparing MD5 checksums...")

            # If the MD5 checksum after 3 mins is the same as before, file is done acquiring
            if new_md5 == old_md5:
                print("MD5 checksums matched. Preparing to process file.")
                time.sleep(180)
                return True
            else:
                db.update_md5_checksum(self.instrument_id, filename, new_md5)


    def trigger_pipeline(self, path, filename, extension):

        """
        Routes data file to monitoring and processing functions
        """

        print("Watching file:", filename)

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
            print("Data processing for", filename, "complete.")

        # Check if data file was the last sample in the sequence
        if filename == self.filenames[-1]:
            # If so, stop acquisition listening
            print("Last sample acquired. Instrument run complete.")
            self.observer.stop()

            # Terminate acquisition listener process
            print("Terminating acquisition listener process.")
            terminate_job(self.instrument_id, self.run_id)


def start_listener(path, instrument_id, run_id):

    """
    Watchdog file monitor to get files in directory upon data acquisition completion
    """

    print("Run monitoring initiated for", path)

    # Check if MS-AutoQC job type is active monitoring or bulk QC
    is_completed_run = db.is_completed_run(instrument_id, run_id)

    # Retrieve filenames for samples in run
    filenames = db.get_remaining_samples(instrument_id, run_id)

    # Get data file extension
    extension = db.get_data_file_type(instrument_id)

    # Format acquisition path
    path = path.replace("\\", "/")
    path = path + "/" if path[-1] != "/" else path

    if is_completed_run:

        # Iterate through files and process each one
        for filename in filenames:

            # If file is not in directory, skip it
            full_path = path + filename + "." + extension
            if not os.path.exists(full_path):
                continue

            # Process data file
            qc.process_data_file(path, filename, extension, instrument_id, run_id)
            print("Data processing for", filename, "complete.")

        print("Last sample acquired. QC job complete.")
        terminate_job(instrument_id, run_id)

    else:
        # Get samples that may have been unprocessed due to an error or accidental termination
        missing_samples, current_sample = db.get_unprocessed_samples(instrument_id, run_id)

        # Check for missed samples and process them before starting file monitor
        if len(missing_samples) > 0:

            # Iterate through files and process each one
            for filename in missing_samples:

                # If file is not in directory, skip it
                full_path = path + filename + "." + extension
                if not os.path.exists(full_path):
                    continue

                # Process data file
                qc.process_data_file(path, filename, extension, instrument_id, run_id)
                print("Data processing for", filename, "complete.")

        # Start file monitor and process files as they are created
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        observer = Observer()
        event_handler = DataAcquisitionEventHandler(observer, path, filenames, extension, instrument_id, run_id, current_sample)
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
    db.delete_temp_directory(instrument_id, run_id)

    # Kill acquisition listener
    pid = db.get_pid(instrument_id, run_id)
    qc.kill_subprocess(pid)


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
    start_listener(path=sys.argv[1], instrument_id=sys.argv[2], run_id=sys.argv[3])