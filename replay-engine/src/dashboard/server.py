from flask import Flask, render_template, jsonify, request
import os
import json
import requests
from datetime import datetime

app = Flask(__name__, template_folder='static', static_folder='static')

# Global config (from env or hardcode for now)
API_BASE = "http://localhost:8000"
TOKEN = "Bearer mysecret"

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start_replay():
    """Start replay via API"""
    try:
        resp = requests.post(
            f"{API_BASE}/replay/start",
            headers={'Content-Type': 'application/json', 'Authorization': TOKEN},
            json={'mode': 'dry-run'}
        )
        if resp.status_code == 200:
            data = resp.json()
            return jsonify({'status': 'started', 'replay_id': data['replay_id']})
        return jsonify({'error': 'Failed to start'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/status/<replay_id>')
def get_status(replay_id):
    """Get replay status via API"""
    try:
        resp = requests.get(
            f"{API_BASE}/replay/status?replay_id={replay_id}",
            headers={'Authorization': TOKEN}
        )
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify({'error': 'Status not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/report/<replay_id>')
def get_report(replay_id):
    """Load report from file"""
    report_path = f'../reports/{replay_id}_summary.json'  # Relative to /app/src/dashboard
    if os.path.exists(report_path):
        with open(report_path, 'r') as f:
            return jsonify(json.load(f))
    return jsonify({'error': 'Report not found'}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)