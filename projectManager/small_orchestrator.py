#!/usr/bin/python
import glob, os
import subprocess
from datetime import date, timedelta,datetime
JAR_FILE = "DockerSparkPipeline-assembly-0.8.jar"
MAIN_FILE = "orchestrator.FutureOrchestrator"
CONF_FILE = "conf/textPipeline.conf"
INPUTS_FOLDER  = "/var/data/inputs/projects/"

def main():
    output = open("log_small_orchestrator.log", "a")
    print "Starting small orchestrator"
    output.write("[%s]: Starting small orchestrator\n" % now())
    projects = [1,2,3,4,5,8,9,10]
    today = date.today()
    yesterday = today - timedelta(1)
    datestr = yesterday.strftime("%Y-%m-%d")
    for project in projects:
        directory = "%s%s/%s/twitter/" % (root_folder, datestr, project)
        input_files = get_input_files(directory)
        for input_file in input_files:
            command = "java -cp %s %s %s %s" % (JAR_FILE, MAIN_FILE, CONF_FILE, input_file)
            print "Executing '%s'" % command
            output.write("[%s]: Execution '%s'\n" % (now(), command))
            code = subprocess.call(command.split(" "))
            if code == 0:
                output.write("[%s]: Success!\n"% now())
            else:
                output.write("[%s]: Error!!!!!\n"% now())
    print "----small orchestrator finished"
    output.write("[%s]: Finished\n"% now())

def get_input_files(directory):
    os.chdir(directory)
    return ["%s%s" %(directory, file) for file in glob.glob("*.txt")]

def now():
    #return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    print "Now"

if __name__ == "__main__":
    main()
