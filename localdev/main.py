from datetime import datetime
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import UUID
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from typing import Optional

# Connect to the local ScyllaDB instance
cluster = Cluster(contact_points=['127.0.0.1'])
session = cluster.connect()

# Create the keyspace and table if they don't exist
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS media_player WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('media_player')
session.execute("""
    CREATE TABLE IF NOT EXISTS songs (
        id UUID PRIMARY KEY,
        title TEXT,
        album TEXT,
        artist TEXT,
        created_at TIMESTAMP
    )
""")

class Song(BaseModel):
    id: Optional[UUID] =uuid.uuid4(),
    title: str
    album: str
    artist: str
    created_at: datetime =datetime.now()

app = FastAPI()

@app.get("/")
def home():
    return {"status": "success", "message": "Hey, it's working well :)"}

@app.get("/song")
def get_songs():
    query = "SELECT * FROM songs"
    results = session.execute(query)
    res=[]
    for s in results:
        res.append(s)
    return {"status":"sucess","message":"fetched all songs details","results":res}

@app.post("/song")
def post_song(song: Song):
    query = session.prepare("""
    INSERT INTO songs (id, title, album, artist, created_at) VALUES (?, ?, ?, ?, ?)""")
    obj = []
    song.id = uuid.uuid4()
    obj.append(song.id)
    obj.append(song.title)
    obj.append(song.album)
    obj.append(song.artist)
    obj.append(song.created_at)
    session.execute(query,obj)
    
    return obj
