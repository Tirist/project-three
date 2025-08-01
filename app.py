#!/usr/bin/env python3
"""
Flask web server for Stock Evaluation Pipeline
Deployed on Google Cloud Run
"""

import os
import subprocess
import json
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def health_check():
    """Health check endpoint for Cloud Run."""
    return jsonify({
        'status': 'healthy',
        'service': 'stock-pipeline',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/run', methods=['POST'])
def run_pipeline():
    """Run the stock pipeline with specified parameters."""
    try:
        # Get parameters from request
        data = request.get_json() or {}
        mode = data.get('mode', 'test')  # test, prod, full
        storage_provider = data.get('storage_provider', 'local')  # local, gcs, s3, azure
        
        # Build command
        cmd = ['python', 'pipeline/run_pipeline.py']
        
        if mode == 'test':
            cmd.append('--test')
        elif mode == 'prod':
            cmd.append('--prod')
        elif mode == 'full':
            cmd.append('--full')
        
        if storage_provider != 'local':
            cmd.extend(['--storage-provider', storage_provider])
            # Add storage config for cloud providers
            cmd.extend(['--storage-config', 'config/cloud_settings.yaml'])
        
        # Run pipeline
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        return jsonify({
            'status': 'success' if result.returncode == 0 else 'error',
            'return_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'command': ' '.join(cmd),
            'timestamp': datetime.now().isoformat()
        })
        
    except subprocess.TimeoutExpired:
        return jsonify({
            'status': 'error',
            'message': 'Pipeline execution timed out',
            'timestamp': datetime.now().isoformat()
        }), 408
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/status')
def get_status():
    """Get pipeline status and configuration."""
    return jsonify({
        'service': 'stock-pipeline',
        'version': '1.0.0',
        'environment': {
            'alpha_vantage_key': 'configured' if os.getenv('ALPHA_VANTAGE_API_KEY') else 'not_configured',
            'google_credentials': 'configured' if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') else 'not_configured',
            'storage_provider': 'local'  # default
        },
        'endpoints': {
            'health': '/',
            'run_pipeline': '/run (POST)',
            'status': '/status'
        },
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Get port from environment variable (Cloud Run sets PORT=8080)
    port = int(os.environ.get('PORT', 8080))
    
    # Run Flask app
    app.run(host='0.0.0.0', port=port, debug=False) 