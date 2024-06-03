"""
This is the file for Load Testing using Locust

"""

from locust import HttpUser, TaskSet, task, between

class UserBehavior(TaskSet):
    @task
    def get_manifest(self):
        self.client.get("/manifest/1280x720")

    @task
    def pause_stream(self):
        self.client.post("/pause/pause123?resolution=1280x720")

    @task
    def resume_stream(self):
        self.client.post("/resume/pause123")

class WebsiteUser(HttpUser):
    tasks = [UserBehavior]
    wait_time = between(1, 2)
