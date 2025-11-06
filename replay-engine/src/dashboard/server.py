import os
import json
import time
import threading
from datetime import datetime
from flask import Flask, render_template, send_from_directory, jsonify, request, send_file
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import requests
import redis
from pathlib import Path

# Rest of your server.py code...

app = Flask(__name__, static_folder='static', template_folder='static')
app.config['SECRET_KEY'] = 'replay-dashboard-secret'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Configuration
CONTROL_API_URL = os.getenv('CONTROL_API_URL', 'http://localhost:8000')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
REPLAY_TOKEN = os.getenv('REPLAY_SHARED_TOKEN', 'mysecret')

# Redis client
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    redis_connected = True
except Exception as e:
    print(f"Redis connection failed: {e}")
    redis_client = None
    redis_connected = False

# Global state
current_replay_status = {
    'running': False,
    'replay_id': None,
    'progress': 0,
    'events_processed': 0,
    'bugs_detected': 0,
    'elapsed': 0
}
session_history = []

# Background thread for status polling
def status_polling_thread():
    """Poll control API for replay status and emit socket updates"""
    prev_events = 0
    last_event_str = ""  # Track last event to avoid duplicates
    
    while True:
        try:
            if current_replay_status['running'] and current_replay_status['replay_id']:
                # Get status from control API
                response = requests.get(
                    f"{CONTROL_API_URL}/replay/status",
                    params={'replay_id': current_replay_status['replay_id']},
                    headers={'Authorization': f'Bearer {REPLAY_TOKEN}'},
                    timeout=5
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Update global state
                    current_replay_status['progress'] = data.get('progress', 0)
                    current_replay_status['events_processed'] = data.get('events_processed', 0)
                    current_replay_status['bugs_detected'] = data.get('bugs_detected', 0)
                    current_replay_status['elapsed'] = data.get('elapsed_seconds', 0)
                    
                    # Build rich event string from current_event_details
                    event_details = data.get('current_event_details', {})
                    if event_details and event_details.get('path') != 'Unknown':
                        current_event_rich = f"{event_details.get('method', 'GET')} {event_details.get('path', 'Unknown')} - {event_details.get('activity', 'N/A')} ({event_details.get('status', 'N/A')})"
                    else:
                        current_event_rich = data.get('current_event_id', data.get('message', 'Processing...'))
                    
                    # Determine event type
                    event_type = 'success'
                    status_code = event_details.get('status', 200)
                    try:
                        if isinstance(status_code, (int, str)):
                            code = int(status_code)
                            if code >= 400:
                                event_type = 'error'
                            elif code >= 300:
                                event_type = 'warning'
                    except:
                        pass
                    
                    # Calculate delta
                    event_delta = current_replay_status['events_processed'] - prev_events
                    
                    # CRITICAL FIX: Emit on EVERY poll during active replay
                    # AND ensure new events are added to stream
                    if current_replay_status['events_processed'] > prev_events and current_event_rich != last_event_str:
                        socketio.emit('update', {
                            'progress': current_replay_status['progress'],
                            'events_processed': current_replay_status['events_processed'],
                            'bugs_detected': current_replay_status['bugs_detected'],
                            'elapsed': current_replay_status['elapsed'],
                            'current_event': current_event_rich,
                            'event_type': event_type,
                            'event_delta': event_delta,
                            'force_add': True  # Flag to force adding to stream
                        })
                        
                        last_event_str = current_event_rich
                        prev_events = current_replay_status['events_processed']
                        
                        print(f"📤 Emitted event {prev_events}: {current_event_rich}")
                    
                    # Check if completed
                    if data.get('state') == 'completed' or current_replay_status['progress'] >= 1.0:
                        current_replay_status['running'] = False
                        
                        # Add to session history
                        if not any(s.get('replay_id') == current_replay_status['replay_id'] for s in session_history):
                            session_history.append({
                                'replay_id': current_replay_status['replay_id'],
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'events': current_replay_status['events_processed'],
                                'duration': f"{current_replay_status['elapsed']}s"
                            })
                        
                        socketio.emit('completed')
                        print(f"✅ Replay completed: {current_replay_status['replay_id']}")
                        
        except Exception as e:
            print(f"❌ Status polling error: {e}")
            if current_replay_status['running']:
                socketio.emit('error', {'message': str(e)})
        
        time.sleep(0.5)  # Poll every 0.5 seconds (faster polling)

# Start background thread
polling_thread = threading.Thread(target=status_polling_thread, daemon=True)
polling_thread.start()

# Routes
@app.route('/')
def index():
    """Serve main dashboard HTML"""
    return send_from_directory('static', 'index.html')

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    api_connected = False
    try:
        response = requests.get(f"{CONTROL_API_URL}/health", timeout=3)
        api_connected = response.status_code == 200
    except:
        pass
    
    return jsonify({
        'api_connected': api_connected,
        'redis_connected': redis_connected,
        'status': 'healthy' if (api_connected and redis_connected) else 'degraded'
    })

@app.route('/api/replay/start', methods=['POST'])
def start_replay():
    """Start a new replay"""
    try:
        data = request.json or {}
        mode = data.get('mode', 'dry-run')
        
        # Call control API WITH AUTH HEADER
        response = requests.post(
            f"{CONTROL_API_URL}/replay/start",
            json={'mode': mode},
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {REPLAY_TOKEN}'  # THIS IS CRITICAL
            },
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            replay_id = result.get('replay_id')
            
            # Update global state
            current_replay_status['running'] = True
            current_replay_status['replay_id'] = replay_id
            current_replay_status['progress'] = 0
            current_replay_status['events_processed'] = 0
            current_replay_status['bugs_detected'] = 0
            current_replay_status['elapsed'] = 0
            
            return jsonify({
                'success': True,
                'replay_id': replay_id,
                'message': 'Replay started successfully'
            })
        else:
            return jsonify({
                'success': False,
                'error': response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/replay/stop', methods=['POST'])
def stop_replay():
    """Stop current replay"""
    try:
        data = request.json or {}
        replay_id = data.get('replay_id', current_replay_status.get('replay_id'))
        
        if not replay_id:
            return jsonify({'success': False, 'error': 'No active replay'}), 400
        
        # Call control API
        response = requests.post(
            f"{CONTROL_API_URL}/replay/stop",
            json={'replay_id': replay_id},
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {REPLAY_TOKEN}'
            },
            timeout=10
        )
        
        if response.status_code == 200:
            current_replay_status['running'] = False
            return jsonify({
                'success': True,
                'message': 'Replay stopped'
            })
        else:
            return jsonify({
                'success': False,
                'error': response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/replay/status')
def get_replay_status():
    """Get current replay status"""
    try:
        replay_id = request.args.get('replay_id', current_replay_status.get('replay_id'))
        
        if not replay_id:
            return jsonify({
                'status': 'idle',
                'message': 'No active replay'
            })
        
        # Call control API
        response = requests.get(
            f"{CONTROL_API_URL}/replay/status",
            params={'replay_id': replay_id},
            headers={'Authorization': f'Bearer {REPLAY_TOKEN}'},
            timeout=5
        )
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({
                'error': 'Failed to get status'
            }), response.status_code
            
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/replay/history')
def get_replay_history():
    """Get recent replay sessions"""
    return jsonify({
        'sessions': session_history[-10:]  # Last 10 sessions
    })

@app.route('/api/export')
def export_report():
    """Export latest replay report"""
    try:
        # Look for latest report file
        reports_dir = Path(__file__).parent.parent / 'reports'
        report_file = reports_dir / 'replay_summary.json'
        
        if report_file.exists():
            return send_file(
                report_file,
                mimetype='application/json',
                as_attachment=True,
                download_name=f'replay_report_{int(time.time())}.json'
            )
        else:
            return jsonify({
                'error': 'No report found'
            }), 404
            
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/metrics')
def get_metrics():
    """Get current metrics (for debugging)"""
    metrics = {
        'current_status': current_replay_status,
        'session_count': len(session_history),
        'redis_connected': redis_connected,
        'api_url': CONTROL_API_URL
    }
    
    if redis_client:
        try:
            metrics['redis_stream_length'] = redis_client.xlen('logs:stream')
        except:
            metrics['redis_stream_length'] = 'error'
    
    return jsonify(metrics)

# Socket.IO events
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    emit('connected', {'message': 'Connected to dashboard server'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnect"""
    print('Client disconnected')

@socketio.on('request_status')
def handle_status_request():
    """Handle manual status request from client"""
    emit('update', {
        'progress': current_replay_status['progress'],
        'events_processed': current_replay_status['events_processed'],
        'bugs_detected': current_replay_status['bugs_detected'],
        'elapsed': current_replay_status['elapsed']
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting 8050 Replay Control Dashboard")
    print("=" * 60)
    print(f"📡 Control API: {CONTROL_API_URL}")
    print(f"💾 Redis: {REDIS_URL}")
    print(f"🔌 Redis Connected: {redis_connected}")
    print(f"🌐 Dashboard: http://localhost:8050")
    print("=" * 60)
    
    socketio.run(app, host='0.0.0.0', port=8050, debug=True, allow_unsafe_werkzeug=True)