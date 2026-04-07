from flask import Flask, request, jsonify
from flask_socketio import SocketIO, join_room, leave_room, emit
from flask_cors import CORS
import requests
import os
import uuid
import time
import threading

YT_API_KEY = os.environ.get('YT_API_KEY', '')

# ── Search result cache (reduces YouTube API quota burn across many users) ──────
_search_cache      = {}
_search_cache_lock = threading.Lock()
CACHE_TTL          = 300  # seconds

def _cache_get(key):
    with _search_cache_lock:
        entry = _search_cache.get(key)
        if entry and (time.time() - entry['ts']) < CACHE_TTL:
            return entry['data']
        return None

def _cache_set(key, data):
    with _search_cache_lock:
        _search_cache[key] = {'data': data, 'ts': time.time()}
        # Evict expired entries when the cache grows large
        if len(_search_cache) > 500:
            cutoff = time.time() - CACHE_TTL
            for k in [k for k, v in list(_search_cache.items()) if v['ts'] < cutoff]:
                del _search_cache[k]


def _search_innertube(query, max_results):
    """Search YouTube via the Innertube API — no API key or quota required."""
    res = requests.post(
        'https://www.youtube.com/youtubei/v1/search',
        json={
            'context': {
                'client': {
                    'clientName':    'WEB',
                    'clientVersion': '2.20231219.04.00',
                    'hl': 'en', 'gl': 'US'
                }
            },
            'query': query
        },
        headers={'Content-Type': 'application/json', 'Accept-Language': 'en'},
        timeout=8
    )
    res.raise_for_status()
    items = []
    sections = (
        res.json()
        .get('contents', {})
        .get('twoColumnSearchResultsRenderer', {})
        .get('primaryContents', {})
        .get('sectionListRenderer', {})
        .get('contents', [])
    )
    for section in sections:
        for item in section.get('itemSectionRenderer', {}).get('contents', []):
            vr = item.get('videoRenderer')
            if not vr:
                continue
            video_id = vr.get('videoId')
            title    = ''.join(r.get('text', '') for r in vr.get('title', {}).get('runs', []))
            channel  = ''.join(r.get('text', '') for r in vr.get('ownerText', {}).get('runs', []))
            if video_id and title:
                items.append({
                    'videoId': video_id,
                    'title':   title,
                    'thumb':   f'https://img.youtube.com/vi/{video_id}/mqdefault.jpg',
                    'channel': channel,
                })
            if len(items) >= max_results:
                return items
    return items


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


# ── REST: YouTube search proxy (cached + Innertube fallback) ─────────────────
@app.route('/api/search', methods=['GET'])
def search_youtube():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'items': []})
    max_results = min(int(request.args.get('maxResults', 8)), 15)

    cache_key = f"{query.lower()}:{max_results}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify({'items': cached})

    items = []

    # 1️⃣  Official YouTube Data API v3 (costs 100 quota units per call)
    if YT_API_KEY:
        try:
            res = requests.get(
                'https://www.googleapis.com/youtube/v3/search',
                params={'part': 'snippet', 'type': 'video',
                        'maxResults': max_results, 'q': query, 'key': YT_API_KEY},
                timeout=8
            )
            if res.ok:
                data = res.json()
                items = [{
                    'videoId': i['id']['videoId'],
                    'title':   i['snippet']['title'],
                    'thumb':   i['snippet']['thumbnails']['default']['url'],
                    'channel': i['snippet']['channelTitle'],
                } for i in data.get('items', [])]
        except Exception:
            pass

    # 2️⃣  Innertube fallback — no key, no quota, used when official API is
    #     unavailable, quota-exceeded, or YT_API_KEY is not set.
    if not items:
        try:
            items = _search_innertube(query, max_results)
        except Exception as e:
            return jsonify({'error': str(e), 'items': []}), 502

    if items:
        _cache_set(cache_key, items)

    return jsonify({'items': items})


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