from twarc import Twarc
from hdfs import InsecureClient

import sys
import os
import json
import time
import re

#client_key = "4284680837-JRl0SwXnop8nJMUL0QEQ56BwDDXOpoa08tyRNUq"
#client_secret = "JqynNht065BaMG59pljaKnD2Jcql1DKXnE8ExvwC2L9rO"
#access_token = "OyfHS2NOKvefddSSjB24e3a8L"
#access_token_secret = "zDpmCrMNtNCILQDYvS2yw7fCDBVMhym89m2B3s0hVv231EiryQ"

access_token = "4284680837-JRl0SwXnop8nJMUL0QEQ56BwDDXOpoa08tyRNUq"
access_token_secret= "JqynNht065BaMG59pljaKnD2Jcql1DKXnE8ExvwC2L9rO"
client_key = "OyfHS2NOKvefddSSjB24e3a8L"
client_secret = "zDpmCrMNtNCILQDYvS2yw7fCDBVMhym89m2B3s0hVv231EiryQ"

total_keywords = {}

languages = ["es", "en"]

TWEET_DUMP_SIZE = 1000

def main():
	"""
	Main program
	"""
	# argument check
	if len(sys.argv) > 1:
		# if argument file exists
		if os.access(sys.argv[1], os.R_OK):
			input_file = sys.argv[1]
		else:
			sys.stderr.write("ERROR, NEED VALID FILE\n")
			sys.exit(1)
	else:
		sys.stderr.write("ERROR, NEED FILE\n")
		sys.exit(1)

	# check if data folder exists or create it
	if not os.path.isdir("data"):
		os.makedirs("data")

	# keep running stream function (every hour)
	while True:

		# string of streaming words
		keys = ""
		lines = []
                projects = []

		# open file for read
		with open(input_file, "r") as fr:
			for line in fr:
				# empty line
				if line != '\n':
					# remove white chars in start and end of line
					line = line.rstrip('\n\t ')
					line = line.strip('\t ')
					# append line to array and string
                                        keywords = line.split("::")
					keys = keys + ",".join(keywords) + ","
					project = {"name": keywords[0], "keywords":keywords}
                                        projects.append(project)

		keys = keys.rstrip(",")

		# create Twarc class
		t = Twarc(client_key, client_secret, access_token, access_token_secret)

		# call stream function every hour
		if stream(keys, projects, t) != True:
			sys.stderr.write("ERROR, STREAM QUITS\n")
			sys.exit(1)

def stream(string, projects, t):
	"""
	Stream tweets from twitter and save them to file every hour

	Args:
		lines - array of streaming words
		t - Twarc class

	Returns:
		boolean - True (OK) / False (Error)
	"""
	string = string

	hour_keywords = {}

	# make timestamps
	timestr = time.strftime("%Y-%m-%d_%H-%M-%S")
	datestr = time.strftime("%Y-%m-%d")

	# get total time for check time
	start_time = time.time()

	# create directories and files for keywords
        tweets_to_write = {}
        indexes = {}
        client = InsecureClient('http://192.168.1.12:50070', user='stratio')
	for project in projects:
                project_name = project["name"]
		dir_name = project_name.replace(" ", "_")

		# for statistics
		if not os.path.isdir("data/statistics"):
			os.makedirs("data/statistics")

		# for statistics date
		if not os.path.isdir("data/statistics/"+datestr):
			os.makedirs("data/statistics/"+datestr)

		# for keyword
		if not os.path.isdir("data/"+dir_name):
			os.makedirs("data/"+dir_name)

		# for date
		if not os.path.isdir("data/"+dir_name+"/"+datestr):
			os.makedirs("data/"+dir_name+"/"+datestr)

		# create json file for writing data
		with open("data/"+dir_name+"/"+datestr+"/"+timestr+".json", "w") as fw:
			fw.write("[")

                tweets_to_write[dir_name] = []
                indexes[dir_name] = 0
             

	while True:
		try:
			# find lines in twitter
                        print "Query string: %s" % string 
			for tweet in t.stream(string):
				# regex to find keyword
				for project in projects:
                                        project_name = project["name"]
					dir_name = project_name.replace(" ", "_")
                			filename = "data/"+dir_name+"/"+datestr+"/"+timestr
					check = 0
                                        for keyword in project["keywords"]:
						# create list of words in keyword
						wlist = keyword.split()
						# length of this list
						w_length = len(wlist)
						# for every word in keyword
						for w in wlist:
							# check if word is in tweet
							keyword = re.search("%s" % w, tweet["text"], re.IGNORECASE)
							if keyword:
								check += 1
                                                if check== w_length:
                                                    break
					# if every word from keyword is in tweet, save to file
					if check == w_length:
                                                print "Tweet language: %s" % tweet['lang']
                                                if tweet['lang'] in languages:
                                                	dumped_json = json.dumps(tweet)
                                                        tweets_to_write[dir_name].append(dumped_json)
							with open(filename + ".json", "a") as fw:
                                                	    
								fw.write(dumped_json)
								fw.write(",")
                                                	 
                                	        	


							# counting total
							if project_name in total_keywords:
								total_keywords[project_name] += 1
							else:
								total_keywords[project_name] = 1
							# counting hourly
							if project_name in hour_keywords:
								hour_keywords[project_name] += 1
							else:
								hour_keywords[project_name] = 1
                          
                                                        print "Tweets for %s: %s" % (dir_name, len(tweets_to_write[dir_name]))
                                                        if len(tweets_to_write[dir_name]) % TWEET_DUMP_SIZE == 0:
                                            			print "Goint to write into %s_%s" % (filename, indexes[dir_name])
                                                                text_to_write = "\n".join(tweets_to_write[dir_name])
                                	    			with client.write(filename + "_" + str(indexes[dir_name]), encoding='utf-8') as writer:
                                	    			    writer.write(text_to_write)
                                	    			indexes[dir_name] = indexes[dir_name]+1
                                	    			tweets_to_write[dir_name] = []

				# exit every hour and start function again
				if start_time+3600 < time.time():
                                         
					for project in projects:
                                                project_name = project['name']
						dir_name = project_name.replace(" ", "_")
						with open("data/"+dir_name+"/"+datestr+"/"+timestr+".json", "a+") as fw:
							fw.seek(-1, os.SEEK_END)
							if fw.read() == ",":
								fw.seek(-1, os.SEEK_END)
								fw.truncate()
							fw.write("]")
                                                print "Tweets for %s: %s" % (dir_name, len(tweets_to_write[dir_name]))
                                                filename = "data/"+dir_name+"/"+datestr+"/"+timestr
                                                if(len(tweets_to_write[dir_name])>0):
                                                    text_to_write = "\n".join(tweets_to_write[dir_name])
                                  	    	    with client.write(filename + "_" + str(indexes[dir_name]+1), encoding='utf-8') as writer:
                                	    	        writer.write(text_to_write)
					# hour statistics
					with open("data/statistics"+"/"+datestr+"/"+timestr+".txt", "w") as fw:
						for word in hour_keywords:
							fw.write(str(word) + " : " + str(hour_keywords[word]) + "\n")
					# total statistics
					with open("data/statistics/statistics.txt", "w") as fw:
						for word in total_keywords:
							fw.write(str(word) + " : " + str(total_keywords[word]) + "\n")
					return True

		# except for quit application
		except KeyboardInterrupt:
			for project in projects:
                                word = project["name"]
				dir_name = word.replace(" ", "_")
				with open("data/"+dir_name+"/"+datestr+"/"+timestr+".json", "a+") as fw:
					fw.seek(-1, os.SEEK_END)
					if fw.read() == ",":
						fw.seek(-1, os.SEEK_END)
						fw.truncate()
					fw.write("]")
			# hour statistics
			with open("data/statistics"+"/"+datestr+"/"+timestr+".txt", "w") as fw:
				for word in hour_keywords:
					fw.write(str(word) + " : " + str(hour_keywords[word]) + "\n")
			# total statistics
			with open("data/statistics/statistics.txt", "w") as fw:
				for word in total_keywords:
					fw.write(str(word) + " : " + str(total_keywords[word]) + "\n")
			sys.stdout.write("QUIT\n")
			sys.exit(0)
		# except for problems with key
		except KeyError:
			# exit every hour and start function again
			if start_time+3600 < time.time():
				for project in projects:
                                        word = project["name"]
					dir_name = word.replace(" ", "_")
					with open("data/"+dir_name+"/"+datestr+"/"+timestr+".json", "a+") as fw:
						fw.seek(-1, os.SEEK_END)
						if fw.read() == ",":
							fw.seek(-1, os.SEEK_END)
							fw.truncate()
						fw.write("]")
				# hour statistics
				with open("data/statistics"+"/"+datestr+"/"+timestr+".txt", "w") as fw:
					for word in hour_keywords:
						fw.write(str(word) + " : " + str(hour_keywords[word]) + "\n")
				# total statistics
				with open("data/statistics/statistics.txt", "w") as fw:
					for word in total_keywords:
						fw.write(str(word) + " : " + str(total_keywords[word]) + "\n")
				return True
			continue
	# error
	return False

if __name__ == "__main__":
	main()
