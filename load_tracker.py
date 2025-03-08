from fastapi import FastAPI
import asyncio
import time
from typing import Dict, List
import os

class LoadTracker:
    def __init__(self):
        self.active_tasks: int = 0
        self.max_tasks: int = int(os.getenv("MAX_TASKS", "10"))
        self.task_history: List[Dict] = []
        
    def increment_tasks(self):
        self.active_tasks += 1
        self.task_history.append({"timestamp": time.time(), "action": "start"})
        # Clean history older than 5 minutes
        cutoff = time.time() - 300
        self.task_history = [t for t in self.task_history if t["timestamp"] > cutoff]
        
    def decrement_tasks(self):
        if self.active_tasks > 0:
            self.active_tasks -= 1
        self.task_history.append({"timestamp": time.time(), "action": "end"})
        
    def is_busy(self):
        return self.active_tasks >= self.max_tasks
        
    def get_status(self):
        return {
            "active_tasks": self.active_tasks,
            "max_tasks": self.max_tasks,
            "is_busy": self.is_busy(),
            "recent_task_count": len([t for t in self.task_history if t["action"] == "start" and t["timestamp"] > time.time() - 60])
        }
        
# Create a global instance
load_tracker = LoadTracker()

def setup_load_tracking(app: FastAPI):
    @app.get("/status/")
    async def get_status():
        return load_tracker.get_status()
        
    return load_tracker