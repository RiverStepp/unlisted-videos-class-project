#available at Invoke-RestMethod http://localhost:8000/metrics

from fastapi import FastAPI, HTTPException
import json
from pathlib import Path
import uvicorn

class MetricsAPI:
    def __init__(self, metrics_file="metrics_log.json"):
        self.metrics_file = metrics_file

    def run(self):
        app = FastAPI()
        metrics_path = Path(self.metrics_file)

        @app.get("/metrics")
        def get_metrics():
            if not metrics_path.exists():
                raise HTTPException(status_code=404, detail="Metrics not found")
            with metrics_path.open("r", encoding="utf-8") as f:
                return json.load(f)

        uvicorn.run(app, host="0.0.0.0", port=8000)