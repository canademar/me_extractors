import sys
import re
import time
from datetime import datetime, timedelta
import json
import solr as solrpy
from hdfs import InsecureClient
import html2text

#SOLR_INDEX = 'http://mixedadmin:7983/solr/mentions'
SOLR_INDEX = 'http://144.76.155.175:5555/solr'
#SOLR_INDEX = "http://search.crawler.strat.io/solr/collection1"
PAGE_SIZE = 1000
MAX_RESULTS = 10000
#SEARCH_FIELD = 'text'
SEARCH_FIELD = 'pageContent'
#DATE_FIELD = 'created_at'
DATE_FIELD = 'timeDownloaded'
CRAWL_BEGINNING_TIME = '2015-01-01T00:00:00Z'
SAVE_FOLDER = "data/projects/"
HDFS_URL = 'http://192.168.1.12:50070'
HDFS_USER  = 'stratio'
FIELDS = "url,timeDownloaded,pageContent,encoding,charset"


def query_solr(query, start_time, end_time, page_start=0, page_limit=None):
    """Page start and page limit can be used to digest chunk of the results, 
       in order to avoid too much info in memory"""
    solr = solrpy.SolrConnection(SOLR_INDEX)
    results = []   
    i = page_start    
    while(len(results)<MAX_RESULTS):
        start = i*PAGE_SIZE
        full_query = "%s AND %s:[%s TO %s]" % (query, DATE_FIELD, start_time, end_time)
        print("Query %s" % full_query)
        response = solr.query(full_query, **{'rows':PAGE_SIZE, 'start':start, 'fields':FIELDS})
        print("Hits: %s" % response.numFound)
        rows = []
        for doc in response.results:
            doc["timeDownloaded"] = parse_solr_time(str(doc["timeDownloaded"]))
            doc["raw"] = clean_html(doc.pop("pageContent"))
            rows.append(doc)
          
        results += rows
        i += 1
        print(len(results))
        if(len(rows)<PAGE_SIZE):
            print("Hit PAGE_SIZE")
            break
        if(page_limit and page_limit<=i-page_start):
            print("HIT PAGE_LIMIT")
            break
    
    return results

def extract_mentions(docs, keywords):
    result_docs = []    
    for doc in docs:
        text = doc["raw"]
        found_paragraphs = []
        paragraphs = text.split("\n\n")
        for paragraph in paragraphs:
            lw_paragraph = paragraph.lower()
            for keyword in keywords:
                if keyword.lower() in lw_paragraph:
                    found_paragraphs.append(paragraph)
                    result_doc = doc.copy()
                    result_doc["text"] = paragraph
                    result_doc["synonym_found"] = keyword
                    result_docs.append(result_doc)
    return result_docs

def format_docs(docs, project):
    result_docs = []
    for doc in docs: 
        doc["project_id"] = project["id"]
        doc["source"] = "web"
        doc["brand"] = project["name"]
        doc["synonyms"] = project["synonyms"]
        doc["nots"] = project["nots"]
        doc["lang"] = "es"
        result_docs.append(doc)
    return result_docs
    
def read_projects(conf_path):
    with open(conf_path, "r") as fr:
        projects = json.load(fr)
    #    for line in fr:
    #        # empty line
    #        if line != '\n':
    #            # remove white chars in start and end of line
    #            line = line.rstrip('\n\t ')
    #            line = line.strip('\t ')
    #            # append line to array and string
    #            keywords = line.split("::")[1:]
    #            project_id = line.split("::")[0]
    #            project = {"id":project_id, "name": keywords[0], "keywords":keywords}
    #            projects[project_id] = project
    result = {}
    for project in projects:
        result[str(project["id"])] = project
    return result

def execute_regular_project(project):
    yesterday =  (datetime.now()-timedelta(1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ") 
    execute_project(project, yesterday, now)

def execute_new_project(project):
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ") 
    execute_project(project, CRAWL_BEGINNING_TIME, now)

def execute_project(project, start_time, end_time):
    query = '%s:("%s")' % (SEARCH_FIELD, '" OR "'.join(project['synonyms']))
    docs = query_solr(query, start_time, end_time)
    print("Found %s docs for project %s" % (len(docs), project['id']))
    docs = format_docs(docs, project)
    docs = extract_mentions(docs, project['synonyms'])
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
    return SAVE_FOLDER+str(project['id'])+"/"+datestr+"/paradigma/"+timestr

def clean_html(html_text):
    h = html2text.HTML2Text()
    h.ignore_links = True
    h.images_to_alt = True
    h.ignore_emphasis = True
    h.escape_all = True
    clean_text = h.handle(html_text)
    return clean_text

def parse_solr_time(solr_time):
    solr_time = re.sub("\..*", "", solr_time) 
    solr_time = re.sub("\+.*", "", solr_time) 
    date = datetime.strptime(solr_time, "%Y-%m-%d %H:%M:%S")
    return date.strftime("%Y%m%d%H%M%S")

def usage(sys):
    print("Usage: %s conf_file_path {new_project|project|all_projects} {project_id if not all_projects}" % sys.argv[0])

def main():
    if(len(sys.argv) != 3 and len(sys.argv)!=4):
        usage(sys)
        exit(1)
    projects = read_projects(sys.argv[1])
    if sys.argv[2] == "new_project":
        if len(sys.argv) ==4:
            project_id = sys.argv[3]
            if project_id in projects.keys():
                execute_new_project(projects[project_id])
            else:
                print("No project with id %s" % project_id)
                exit(1)
        else:
            usage(sys)
    elif sys.argv[1] == "project":
        if len(sys.argv) ==4:
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
