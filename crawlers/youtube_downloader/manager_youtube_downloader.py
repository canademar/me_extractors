#Encoding: UTF-8
import subprocess 
from datetime import datetime, timedelta
import json

BASE_DIR="/var/data/inputs/videos/"
SEARCH_DIR=BASE_DIR+"%s/%s/search/"
INFO_DIR=BASE_DIR+"%s/%s/video_info/"
VIDEO_DIR=BASE_DIR+"%s/%s/video_files/"

DOWNLOAD_COMMAND = "python youtube_downloader.py --videoId=%s --fetch --project_id=%s --project_name=%s --outputdir=%s"

#python youtube_downloader.py --videoId=2oOdQFd1KVc  --fetch --project_id=9 --project_name=Samsung --outputdir
def download_video(project, video_info, video_date):
    video_id=video_info['video_id']
    project_id = project['id']
    project_name = project['name']
    output_video_dir = VIDEO_DIR % (video_date.replace("T00:00:00Z", ""), project_id)
    subprocess.check_call(["mkdir", "-p", output_video_dir])
    command =  DOWNLOAD_COMMAND % (video_id, project_id, project_name, output_video_dir)
    print command
    result = subprocess.check_output(command.split(" "))
    output_info_dir = INFO_DIR % (video_date.replace("T00:00:00Z", ""), project_id)
    print "Info dir: %s" % output_info_dir
    subprocess.check_call(["mkdir", "-p", output_info_dir])
    with open(output_info_dir+video_id+".json",'w') as outfile:
        outfile.write(result)
    parsed_result = json.loads(result)
    return parsed_result
    


def download_videos(project, video_infos):
    for video_info in video_infos['videos']:
        download_video(project, video_info, video_infos['date'])


#python youtube_downloader.py --search=Samsung --date=2016-11-23T00:00:00Z
def search_videos(project, search_date):
    keyword=project['keyword']
    project_id=project['id']
    command = "python youtube_downloader.py --search=%s --date=%s" % (keyword, search_date)
    print "Searching with: '%s'" % command
    result = subprocess.check_output(command.split(" "))
    output_dir = SEARCH_DIR % (search_date.replace("T00:00:00Z",""), project_id)
    subprocess.check_call(["mkdir", "-p", output_dir])
    with open(output_dir+now_str()+".json",'w') as outfile:
        outfile.write(result)
    print "Search finished at: '%s'" % output_dir
    parsed_result = json.loads(result)
    return parsed_result
    

def get_projects():
    project = {"keyword":"Samsung", "id":9, "name":"Samsung"}
    return [project]

def yesterday_str():
    yesterday = datetime.now() - timedelta(1)
    return yesterday.strftime("%Y-%m-%dT00:00:00Z")

def now_str():
    now = datetime.now()
    return now.strftime("%Y-%m-%d_%H-%M-%S")

def main():
    print "Starting youtube manager"
    projects = get_projects()
    search_date = yesterday_str()
    for project in projects:
        #video_infos = search_videos(project, search_date)
        video_text = '{"date": "2016-11-23T00:00:00Z", "videos": [{"view_count": "314", "description": "Mời các bạn truy cập website: http://clickbuy.com.vn để tham khảo mức giá của rất nhiều dòng sản phẩm như Apple, Samsung, HTC, Sony, LG, Xiaomi... Sony: ...", "title": "Góc Speedtest| Samsung Galaxy On7 & Redmi 4 Prime| Cùng chip s625 máy nào mạnh hơn??", "video_id": "vRBwzXGyNpk", "dislike_count": "1", "channel_id": "UCFoD8gJdm_ID3Qw3-yU8iyw", "like_count": "35", "published": "2016-11-24T13:00:04.000Z"}]}'
        video_infos = json.loads(video_text)
        
        print video_infos['videos']
        print download_videos(project, video_infos)


if __name__=='__main__':
    main()
