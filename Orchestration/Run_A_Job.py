import requests,json, pandas as pd,datetime,sys
import unittest, time

###########################################################
######################Parms for Job########################

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
###########################################################
if len(sys.argv)    >1:
    project         = sys.argv[1]
    org             = sys.argv[2]
    user            = sys.argv[3]
    pwd             = sys.argv[4]
    token           = sys.argv[5]
    asset_type      = sys.argv[6]
    jobid           = sys.argv[7]
    job_path        = sys.argv[8]
    cluster         = sys.argv[9]
    job_name        = sys.argv[10]
    poll_interval   = int(sys.argv[11])# time in seconds to poll for status default is 60


# If you want to hardcode values or read from a parameter file #
else :
    project         = "Daily_Retail_Aggregate" # project folder you want to get data from
    org             = "adb-8446945302740384.4.azuredatabricks.net" # org url of your workspace <field is mandatory>
    user            = "email@saveonfoods.com" # email of identity that will generate token
    pwd             = "" # pwd for user
    token           = "" #token from db workspace <field is mandatory when not generating token>
    asset_type      = []
    jobid           = '1'
    job_path        = ''#location of workbook this <field is mandatory>
    cluster         = ''#id of cluster that will run workbook <field is mandatory>
    job_name        = ''#Name of job being run only for sys out purposes
    poll_interval   = int(60) # time in seconds to poll for status default is 60 

###########################################################

class DB_API():
    def __init__(self, org, user, pwd, token = None, run_type=None, jobid=None, job_path = None,jobtype=None,cluster = None, job_name= None, poll_interval = 60):
        self.org            = org
        self.user           = user
        self.pwd            = pwd
        self.token          = token
        self.run_type       = run_type
        self.jobid          = jobid
        self.job_path       = job_path
        self.jobtype        = jobtype
        self.cluster        = cluster
        self.job_name       = job_name
        self.poll_interval  = poll_interval

    @property    
    def auth(self) -> dict:
        ## currently not working due to service principal not being added to the workspace ##
        ## Once Jeff's team adds will get this up and running ##
        try:
            self.url            = f"https://{self.org}/api/2.0/token/create"
            self.payload        = json.dumps({ "comment": "Data Engineering Job token", "lifetime_seconds": 7776000})
            self.headers        = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            self.response       = requests.request("POST", self.url, headers=self.headers, data=self.payload)
            self.json_resp      = self.response.json()
        except Exception as e:
            print(e)
        return {"token": self.SessionId, "URL": self.serverUrl, "orgid": self.orgUuid,"domain":self.domain,"run_domain":self.run_domain}

    @property
    def token_auth(self) -> dict:
                ## currently not working due to service principal not being added to the workspace ##
        ## Once Jeff's team adds will get this up and running ##
        try:
            self.url            = 
            self.payload        = 
            self.headers        = 
            self.response       = 
            self.json_resp      = 
            self.SessionId      = 
        except Exception as e:
            print(e)
        return {"token": self.SessionId, "URL": self.serverUrl}
    
    @property
    def Run_Job(self):
        try:
            self.run_job_url = f"https://{self.org}/api/2.1/jobs/runs/submit"
            self.run_job_headers = {'Content-Type': 'application/json', 'Accept': 'application/json','Authorization': f'Bearer {self.token}'}
            self.run_job_payload = json.dumps({
                "task_key": f"{self.job_name}",
                "description": "Transformation for creating retail aggregate table",
                "depends_on": [],
                "existing_cluster_id": f"{self.cluster}",
                "notebook_task": {
                    "notebook_path": f"{self.job_path}",
                    "base_parameters": {}
                },
                "run_name": "Fact Retail Aggregate",
                "timeout_seconds": 86400,
                "max_retries": 3,
                "min_retry_interval_millis": 2000,
                "retry_on_timeout": "false"
            }, indent=4)
            self.run_job_response = requests.request("POST", self.run_job_url, headers=self.run_job_headers,data=self.run_job_payload)
            self.run_json_resp = json.dumps(self.run_job_response.json(), indent=4)
            self.run_id = json.loads(self.run_json_resp)["run_id"]
            print(f"Starting New job run job id is : {self.run_id}")
            print(f"Job Path :{self.job_path}")
            return self.run_id
        except Exception as e:
            print("Unable to run job \n excpetion is : \n", e)
            exit(-1)
            
    @property
    def Job_Status(self):
        self.job_status_job_id      = self.Run_Job()
        self.job_status_url         = f"https://{self.org}/api/2.0/jobs/runs/get"
        self.job_status_headers     = {'Content-Type': 'application/json', 'Accept': 'application/json','Authorization': f'Bearer {self.token}'}
        self.job_status_payload     = json.dumps({"run_id":self.job_status_job_id})
        self.job_status_response    = requests.request("GET", self.job_status_url, headers=self.job_status_headers, data=self.job_status_payload)
        self.job_status_json_resp   = json.dumps(self.job_status_response.json(), indent=4)
        self.job_status_state       = json.loads(self.job_status_json_resp)["state"]["life_cycle_state"]
        counter = 0

        while True:
            try:
                if counter == 0 or counter % self.poll_interval == 0:
                    print("Status is :", self.job_status_state, '\n', f"Job has been running for : {counter} seconds")
                    print( "Job Number:",json.loads(self.job_status_json_resp)["number_in_job"])

                self.job_status_response    = requests.request("GET", self.job_status_url, headers=self.job_status_headers,data=self.job_status_payload)
                self.job_status_json_resp   = json.dumps(self.job_status_response.json(), indent=4)
                self.job_status_state       = json.loads(self.job_status_json_resp)["state"]["life_cycle_state"]
                self.job_status_start       = datetime.datetime.fromtimestamp(json.loads(self.job_status_json_resp)["start_time"] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                self.job_status_end         = datetime.datetime.fromtimestamp(json.loads(self.job_status_json_resp)["end_time"] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                self.cluster_ini            = json.loads(self.job_status_json_resp)["setup_duration"] /1000

                if self.job_status_state in ['TERMINATED']:
                    self.job_result_state = json.loads(self.job_status_json_resp)["state"]["result_state"]
                    print("Status is :", self.job_result_state, '\n', f"Job has been running for : {counter} seconds")
                    print(f"Cluster initialization took: {self.cluster_ini} second(s)")
                    print(f"Job started @: {self.job_status_start} \n Job Ended @: {self.job_status_end}")

                    exit(0)
                counter += 1

            except Exception as e:
                print("job run failed debug message is :\n", e, '\n', self.job_status_json_resp)
                exit(-1)
        return self.job_status_json_resp
    
    @property
    def list_clusters(self):
        self.url                 = f"https://{self.org}/api/2.0/clusters/list"
        self.payload             = json.dumps({})
        self.headers             = {'Content-Type': 'application/json', 'Accept': 'application/json','Authorization': f'Bearer {self.token}'}
        self.response            = requests.request("GET", self.url, headers=self.headers, data=self.payload)
        self.json_resp           = json.dumps(self.response.json(),indent = 4)
        return self.json_resp

print(DB_API(org=org,pwd=pwd,user=user,token=token,cluster=cluster,jobid=jobid,job_name=job_name,job_path = job_path).Job_Status)
