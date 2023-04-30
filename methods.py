import requests
import json

def create_project(record):
    project = record.get("fields", {}).get("project", None)
    return project

def create_task(record):
    task = record.get("fields", {}).get("issuetype",{})
    return task

def create_user(record):
    user = record.get("fields", {}).get("creator", {})
    return user

def get_jira(record):
    response = requests.get(
        url = record["url"],
        headers = {"Authorization": f"Basic {record['token']}"}
    )

    content = json.loads(response.content)
    
    for issue in content["issues"]:
        yield issue