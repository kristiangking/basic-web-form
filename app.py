from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import uuid
from datetime import datetime
import boto3

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
            url_count = len(data['urls'])
            max_depth = data['depth']

            dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
            table = dynamodb.Table('web_scraper_job_metadata')
            sqs = boto3.client('sqs', region_name='ap-southeast-2')
            queue_url = sqs.get_queue_url(QueueName='job_urls_to_process')['QueueUrl']

            # Step 1: Write to DynamoDB
            table.put_item(Item={
                'job_id': job_id,
                'job_metadata': data,
                'job_remaining_urls': url_count,
                'job_status': 'PENDING',
                'job_url_count': url_count
            })

            # Step 2: Write one SQS message per URL — rollback DynamoDB if this fails
            try:
                for url in data['urls']:
                    sqs.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps({
                            'job_id': job_id,
                            'max_depth': max_depth,
                            'current_depth': 1,
                            'top_level_url': url,
                            'current_url': url
                        })
                    )
            except Exception:
                table.delete_item(Key={'job_id': job_id})
                raise

            # EC2 log path
            with open('/home/ec2-user/submissions.log', 'a') as f:
                f.write(f"{datetime.now()} - jobId={job_id} - {data}\n")

            # Local dev log path - uncomment when testing locally
            # with open('submissions.log', 'a') as f:
            #     f.write(f"{datetime.now()} - jobId={job_id} - {data}\n")

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
