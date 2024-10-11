from datetime import datetime
import uuid
from fastapi import FastAPI
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pydantic import BaseModel
from uuid import UUID,uuid4
from typing import Optional

cluster = Cluster(
    contact_points=[
       "node-0.aws-us-east-1.3071f1258dfa5469dfe4.clusters.scylla.cloud", "node-1.aws-us-east-1.3071f1258dfa5469dfe4.clusters.scylla.cloud", "node-2.aws-us-east-1.3071f1258dfa5469dfe4.clusters.scylla.cloud"
    ],
    auth_provider=PlainTextAuthProvider(username='scylla', password='J7coqY8ETA2PBZj')
)

session = cluster.connect('media_player')

class Song(BaseModel):
    id: Optional[UUID] =uuid.uuid4(),
    title: str
    album: str
    artist: str
    created_at: datetime =datetime.now()


app =FastAPI()

@app.get("/")
def home():
    return {"status":"sucess","message":"Hey it working well :)"}

@app.get("/song")

def song():
    query = session.prepare("SELECT * FROM songs")
    results = session.execute(query)
    res=[]
    for s in results:
        res.append(s)
    return {"status":"sucess","message":"fetched all songs details","results":res}



@app.post("/song")

def postSong(song:Song):
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


# This is one time creation :)

# keyspaceName = "media_player"
# replicationFactor = 3


# session.execute(
#     """
#     CREATE KEYSPACE {}
#         WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '{}'}}
#         AND durable_writes = true;
#     """.format(keyspaceName, replicationFactor)
# )
# session.set_keyspace('media_player')


# tableQuery = """
# CREATE TABLE songs (
#     id uuid,
#     title text,
#     album text,
#     artist text,
#     created_at timestamp,
#     PRIMARY KEY (id, created_at)
# )
# """


# session.execute(tableQuery)