from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import uuid
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import boto3

class FormHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())

        elif parsed.path == '/job-status':
            params = parse_qs(parsed.query)
            job_id = params.get('job_id', [None])[0]

            if not job_id:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "job_id is required"}).encode())
                return

            dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
            table = dynamodb.Table('web_scraper_job_metadata')
            result = table.get_item(Key={'job_id': job_id})

            if 'Item' not in result:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Job not found"}).encode())
                return

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"job_status": result['Item']['job_status']}).encode())

        elif parsed.path == '/job-results':
            params = parse_qs(parsed.query)
            job_id = params.get('job_id', [None])[0]

            if not job_id:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "job_id is required"}).encode())
                return

            dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
            metadata_table = dynamodb.Table('web_scraper_job_metadata')
            results_table = dynamodb.Table('web_scraper_job_results')

            # Step 1: Validate job exists
            metadata = metadata_table.get_item(Key={'job_id': job_id})
            if 'Item' not in metadata:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Invalid Job ID - please enter a valid Job ID"}).encode())
                return

            # Step 2: Check job is COMPLETE
            if metadata['Item']['job_status'] != 'COMPLETE':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Job is not yet complete."}).encode())
                return

            # Step 3: Retrieve image_tags for each URL
            urls = metadata['Item']['job_metadata']['urls']
            job_results = []
            for url in urls:
                result = results_table.get_item(Key={'top_level_url': url, 'job_id': job_id})
                image_tags = result['Item']['image_tags'] if 'Item' in result else []
                job_results.append({'url': url, 'image_tags': image_tags})

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"job_id": job_id, "results": job_results}).encode())

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
