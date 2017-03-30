#!/usr/bin/python
import glob, os
import subprocess
from datetime import date, timedelta,datetime
JAR_FILE = "MixedEmotionsOrchestrator-assembly-1.0.jar"
CONF_FILE = "conf/youtubeAudioProcessing.conf"
INPUTS_FOLDER  = "/var/data/inputs/videos/"

def main():
    output = open("log/log_audio_orchestrator.log", "a")
    print "Starting audio orchestrator"
    output.write("[%s]: Starting audio orchestrator\n" % now())
    projects = [8,9,10]
    today = date.today()
    yesterday = today - timedelta(2)
    datestr = yesterday.strftime("%Y-%m-%d")
    for project in projects:
        print "Working on project %s" % project
        output.write("Working on project %s\n" % project)
        directory = "%s%s/%s/video_info/" % (INPUTS_FOLDER, datestr, project)
    
        input_files = get_input_files(directory)
        for input_file in input_files:
            command = "java -jar %s %s %s" % (JAR_FILE, CONF_FILE, input_file)
            print "Executing '%s'" % command
            output.write("[%s]: Execution '%s'\n" % (now(), command))
            code = subprocess.call(command.split(" "))
            if code == 0:
                output.write("[%s]: Success!\n"% now())
            else:
                output.write("[%s]: Error!!!!!\n"% now())
    print "----audio orchestrator finished"
    output.write("[%s]: Finished\n"% now())

def get_input_files(directory):
    filenames_array = [ filenames for root, dirnames, filenames in os.walk(directory)]
    files  = [val for sublist in filenames_array for val in sublist]
    return ["%s%s" %(directory, file) for file in files if file.endswith(".json")]

def now():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    

if __name__ == "__main__":
    main()
