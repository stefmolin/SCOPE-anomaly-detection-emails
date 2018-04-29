import time
import subprocess
import logging
import schedule
import yaml
import os

# Logging configuration
FORMAT = '[%(levelname)s] [ %(name)s ] %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(os.path.basename(__file__))

def execute(config_file, last_try=False):
    config = yaml.load(open(config_file))
    for command in config['commands']:
        command = command.format(config_file=config_file, is_last_try=last_try)
        logger.info("Running {c}...".format(c=command))
        subprocess.run(command, shell=True)

def run_now(config_folder, last_try=False):
    logger.info("Running scripts immediately...")
    config_files = [x.path for x in os.scandir(config_folder) if x.is_file() and x.name.endswith(".yml")]
    for config_file in config_files:
        execute(config_file, last_try)

def set_schedule(config_folder):
    logger.info("Scheduling script execution...")
    config_files = [x.path for x in os.scandir(config_folder) if x.is_file() and x.name.endswith(".yml")]
    if not config_files: return
    for config_file in config_files:
        config = yaml.load(open(config_file))
        scheduled_times = config['schedule']
        for scheduled_time in scheduled_times[:-1]:
            schedule.every().day.at(scheduled_time).do(execute, config_file)
        last_time = scheduled_times[-1]
        schedule.every().day.at(last_time).do(execute, config_file, last_try=True)
        # This part only to give feedback to the user
        for command in config['commands']:
            for scheduled_time in scheduled_times[:-1]:
                logger.info('Scheduling "{c}" to run at {t}'.format(c=command.format(config_file=config_file, is_last_try=False), t=scheduled_time))
            last_time = scheduled_times[-1]
            logger.info('Scheduling "{c}" to run at {t}'.format(c=command.format(config_file=config_file, is_last_try=True), t=last_time))
    logger.info("Now waiting for next scheduled execution")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    # build and install the scopeR package for the container
    logger.info("Building and installing the scopeR package...")
    command = "Rscript -e 'install.packages(devtools::build(\"scopeR\", vignettes = FALSE, quiet = TRUE), repos = NULL, quiet = TRUE)'"
    completed_process = subprocess.run(command, shell=True)
    returncode = completed_process.returncode
    logger.debug("Return code: {r}".format(r=returncode))
    if returncode != 0:
        logger.info("Unable to build and install scopeR. Exiting...")
    else:
        # allow scripts to run
        config_folder = "commands/run_immediately/"
        run_now(config_folder)
        run_now(config_folder, last_try=True) # Run a second time to test for last try
        config_folder = "commands/scheduled/"
        set_schedule(config_folder)
        logger.info("Exiting")
