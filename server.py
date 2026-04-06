from flask import Flask, request, jsonify
from flask_socketio import SocketIO, join_room, leave_room, emit
from flask_cors import CORS
import requests
import os
import uuid
import time
from dotenv import load_dotenv; load_dotenv()

YT_API_KEY = os.environ.get('YT_API_KEY', '')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dictjam-secret-key')
CORS(app, resources={r"/api/*": {
    "origins": "*",
    "methods": ["GET", "POST", "OPTIONS"],
    "allow_headers": ["Content-Type"]
}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# ── In-memory room store (gevent cooperative multitasking makes this safe) ──
_rooms = {}

def get_room(room_id):
    return _rooms.get(room_id)

def set_room(room_id, data):
    _rooms[room_id] = data


# ── REST: Create a new room ──────────────────────────────────────────────────
@app.route('/api/rooms', methods=['POST'])
def create_room():
    room_id   = uuid.uuid4().hex[:8]
    body      = request.json or {}
    room_name = body.get('name', 'My Jam')[:40]
    set_room(room_id, {
        "id":           room_id,
        "name":         room_name,
        "queue":        [],
        "currentIndex": -1,
        "playbackTime": 0,
        "playerState":  -1,
        "lastSyncedAt": 0,
        "created":      int(time.time())
    })
    return jsonify({"roomId": room_id, "name": room_name}), 201


# ── REST: Room info (existence check) ───────────────────────────────────────
@app.route('/api/rooms/<room_id>', methods=['GET'])
def room_info(room_id):
    room = get_room(room_id)
    if room is None:
        return jsonify({"error": "Room not found"}), 404
    return jsonify({"id": room_id, "name": room.get("name", "Jam"), "exists": True})


# ── REST: YouTube search proxy ───────────────────────────────────────────────
@app.route('/api/search', methods=['GET'])
def search_youtube():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({"items": []})
    max_results = min(int(request.args.get('maxResults', 8)), 15)
    try:
        res = requests.get(
            'https://www.googleapis.com/youtube/v3/search',
            params={'part': 'snippet', 'type': 'video', 'maxResults': max_results, 'q': query, 'key': YT_API_KEY},
            timeout=8
        )
        res.raise_for_status()
        data = res.json()
        items = [{
            'videoId': i['id']['videoId'],
            'title':   i['snippet']['title'],
            'thumb':   i['snippet']['thumbnails']['default']['url'],
            'channel': i['snippet']['channelTitle'],
        } for i in data.get('items', [])]
        return jsonify({'items': items})
    except Exception as e:
        return jsonify({'error': str(e)}), 502


# ── Socket: client joins a room ──────────────────────────────────────────────
@socketio.on('join')
def on_join(data):
    room_id = data.get('roomId', '')
    if not room_id:
        return
    join_room(room_id)
    room = get_room(room_id)
    if room:
        emit('room_state', {
            'queue':        room.get('queue', []),
            'currentIndex': room.get('currentIndex', -1),
            'playbackTime': room.get('playbackTime', 0),
            'playerState':  room.get('playerState', -1),
            'lastSyncedAt': room.get('lastSyncedAt', 0)
        })


# ── Socket: client leaves a room ────────────────────────────────────────────
@socketio.on('leave')
def on_leave(data):
    room_id = data.get('roomId', '')
    if room_id:
        leave_room(room_id)


# ── Socket: queue updated by a client ───────────────────────────────────────
@socketio.on('queue_update')
def on_queue_update(data):
    room_id = data.get('roomId', '')
    room    = get_room(room_id)
    if not room:
        return
    room['queue']        = data.get('queue',        room['queue'])
    room['currentIndex'] = data.get('currentIndex', room['currentIndex'])
    if 'playbackTime' in data:
        room['playbackTime'] = data['playbackTime']
        room['playerState']  = data.get('playerState', 1)
        room['lastSyncedAt'] = time.time()
    set_room(room_id, room)
    emit('queue_update', {
        'queue':        room['queue'],
        'currentIndex': room['currentIndex'],
        'playbackTime': room.get('playbackTime', 0),
        'playerState':  room.get('playerState', -1),
        'lastSyncedAt': room.get('lastSyncedAt', 0)
    }, room=room_id, include_self=False)


# ── Socket: playback position heartbeat ─────────────────────────────────────
@socketio.on('playback_sync')
def on_playback_sync(data):
    room_id = data.get('roomId', '')
    room    = get_room(room_id)
    if not room:
        return
    room['playbackTime'] = data.get('playbackTime', 0)
    room['playerState']  = data.get('playerState', -1)
    room['lastSyncedAt'] = time.time()
    set_room(room_id, room)
    emit('playback_sync', {
        'playbackTime': room['playbackTime'],
        'playerState':  room['playerState'],
        'lastSyncedAt': room['lastSyncedAt']
    }, room=room_id, include_self=False)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)