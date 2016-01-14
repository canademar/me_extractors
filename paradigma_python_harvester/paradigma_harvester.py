import sys
import time
from datetime import datetime, timedelta
import json
import pysolr
from hdfs import InsecureClient

SOLR_INDEX = 'http://mixedadmin:7983/solr/mentions'
PAGE_SIZE = 1000
MAX_RESULTS = 50000
PROJECT_CONF_FILE = '../twitter_crawler_scripts/projects'    
SEARCH_FIELD = 'text'
CRAWL_BEGINNING_TIME = '2012-04-24T00:00:00Z'
SAVE_FOLDER = "data/projects/"
HDFS_URL = 'http://192.168.1.12:50070'
HDFS_USER  = 'stratio'


def query_solr(query, start_time, end_time, page_start=0, page_limit=None):
    """Page start and page limit can be used to digest chunk of the results, 
       in order to avoid too much info in memory"""
    solr = pysolr.Solr(SOLR_INDEX)
    results = []   
    i = page_start    
    while(len(results)<MAX_RESULTS):
        start = i*PAGE_SIZE
        full_query = "%s AND created_at:[%s TO %s]" % (query, start_time, end_time)
        print("Query %s" % full_query)
        response = solr.search(full_query, **{'rows':PAGE_SIZE, 'start':start})
        print("Hits: %s" % response.hits)
        rows = response.docs
        results += rows
        i += 1
        if(len(rows)<PAGE_SIZE):
            break
        if(page_limit and page_limit<=i-page_start):
            break
    
    return results
    
def read_projects():
    projects = {}
    with open(PROJECT_CONF_FILE, "r") as fr:
        for line in fr:
            # empty line
            if line != '\n':
                # remove white chars in start and end of line
                line = line.rstrip('\n\t ')
                line = line.strip('\t ')
                # append line to array and string
                keywords = line.split("::")[1:]
                project_id = line.split("::")[0]
                project = {"id":project_id, "name": keywords[0], "keywords":keywords}
                projects[project_id] = project
    return projects

def execute_regular_project(project):
    yesterday =  (datetime.now()-timedelta(1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ") 
    execute_project(project, yesterday, now)

def execute_new_project(project):
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ") 
    execute_project(project, CRAWL_BEGINNING_TIME, now)

def execute_project(project, start_time, end_time):
    query = '%s:("%s")' % (SEARCH_FIELD, '" OR "'.join(project['keywords']))
    docs = query_solr(query, start_time, end_time)
    print("Found %s docs for project %s" % (len(docs), project['id']))
    if(len(docs)>0):
        save_to_hdfs(docs, project)


def save_to_hdfs(docs, project, index=0):
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    filename = get_filename(project)
    print("Going to write %s into %s_%s" % (len(docs), filename, index))
    text_to_write = "\n".join([json.dumps(doc) for doc in docs])
    with client.write(filename + "_" + str(index), encoding='utf-8') as writer:
        writer.write(text_to_write)

def get_filename(project):
    timestr = time.strftime("%Y-%m-%d_%H-%M-%S")
    datestr = time.strftime("%Y-%m-%d")
    return SAVE_FOLDER+project['id']+"/"+datestr+"/paradigma/"+timestr

def usage(sys):
    print "Usage: %s {new_project|project|all_projects} {project_id if not all_projects}" % sys.argv[0]

def main():
    if(len(sys.argv) != 2 and len(sys.argv)!=3):
        usage(sys)
        exit(1)
    projects = read_projects()
    if sys.argv[1] == "new_project":
        if len(sys.argv) ==3:
            project_id = sys.argv[2]
            if project_id in projects.keys():
                execute_new_project(projects[project_id])
            else:
                print("No project with id %s" % project_id)
                exit(1)
        else:
            usage()
    elif sys.argv[1] == "project":
        if len(sys.argv) ==3:
            project_id = sys.argv[2]
            if project_id in projects.keys():
                execute_regular_project(projects[project_id])
            else:
                print("No project with id %s" % project_id)
                exit(1)
        else:
            usage(sys)
    elif sys.argv[1] == "all_projects" and len(sys.argv)==2:
        for project_id in projects.keys():
            execute_regular_project(projects[project_id])
    else:
        usage(sys)


if __name__ == "__main__":
    main()
