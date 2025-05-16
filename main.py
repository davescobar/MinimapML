from fastapi import FastAPI, HTTPException, Query
from services.ingestor import ReplayIngestor

app = FastAPI()

@app.post("/ingest")
def ingest_replay(
    replay_path: str = Query(..., description="Path to a .dem.bz2 replay file"),
    parser_url: str = Query("http://localhost:5700/upload", description="URL of the parser service")
):
    try:
        ingestor = ReplayIngestor(parser_url)
        events = ingestor.ingest_from_path(replay_path)  # Use the new method
        return {"parsed_events": len(events), "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
