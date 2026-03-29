from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import uuid
from datetime import datetime

class FormHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/scrape':
            length = int(self.headers['Content-Length'])
            body = self.rfile.read(length)
            data = json.loads(body)

            job_id = str(uuid.uuid4())

            with open('/home/ec2-user/submissions.log', 'a') as f:
                f.write(f"{datetime.now()} - jobId={job_id} - {data}\n")

            response = json.dumps({"jobId": job_id})
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def log_message(self, format, *args):
        pass

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 80), FormHandler)
    print('Backend server running on port 80...')
    server.serve_forever()
