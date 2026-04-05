# Import the json library to parse and serialise JSON data
import json
# Import the time library for sleep operations if needed
import time
# Import boto3, the AWS SDK for Python, to interact with AWS services
import boto3
# Import boto3 DynamoDB conditions for building query expressions
from boto3.dynamodb.conditions import Key
# Import BeautifulSoup from bs4 for parsing HTML content
from bs4 import BeautifulSoup
# Import urljoin to resolve relative URLs into absolute URLs
from urllib.parse import urljoin
# Import requests to make HTTP requests to web pages
import requests
# Import ClientError to handle AWS SDK exceptions
from botocore.exceptions import ClientError

# Define the AWS region where all resources are located
REGION = 'ap-southeast-2'
# Define the name of the SQS queue to poll for scraping jobs
QUEUE_NAME = 'job_urls_to_process'
# Define the visibility timeout in seconds (5 minutes) to prevent duplicate processing
VISIBILITY_TIMEOUT = 300

# Create a DynamoDB resource using boto3 in the specified region
dynamodb = boto3.resource('dynamodb', region_name=REGION)
# Create an SQS client using boto3 in the specified region
sqs = boto3.client('sqs', region_name=REGION)

# Get a reference to the web_scraper_job_metadata DynamoDB table
metadata_table = dynamodb.Table('web_scraper_job_metadata')
# Get a reference to the web_scraper_job_results DynamoDB table
results_table = dynamodb.Table('web_scraper_job_results')
# Get a reference to the web_scraper_image_tags DynamoDB table
image_tags_table = dynamodb.Table('web_scraper_image_tags')
# Get a reference to the web_scraper_visited_urls DynamoDB table
visited_urls_table = dynamodb.Table('web_scraper_visited_urls')
# Retrieve the SQS queue URL using the queue name
queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']


def parse_message(body):
    # Define the list of fields that must be present in every SQS message
    required_fields = ['job_id', 'max_depth', 'current_depth', 'top_level_url', 'current_url']
    # Iterate over each required field and raise an error if any are missing
    for field in required_fields:
        # Check if the current field is missing from the message body
        if field not in body:
            # Raise a ValueError with a descriptive message indicating which field is missing
            raise ValueError(f"Missing required field: {field}")


def scrape_page(url):
    # Initialise an empty list to store image src URLs found on the page
    image_tags = []
    # Initialise an empty list to store child URLs found via <a href> links
    child_urls = []
    # Set the starting URL for pagination - begins with the original URL
    next_url = url

    # Loop through pages, following pagination links until there are no more
    while next_url:
        # Make an HTTP GET request to the current URL with a browser-like User-Agent header and a 30 second timeout
        response = requests.get(next_url, timeout=30, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        # Raise an exception if the HTTP response indicates an error
        response.raise_for_status()
        # Parse the HTML response content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all <img> tags that have a src attribute
        for img in soup.find_all('img', src=True):
            # Resolve the image src to a full absolute URL
            full_url = urljoin(next_url, img['src'])
            # Add the resolved image URL to the image_tags list
            image_tags.append(full_url)

        # Find all <a> tags that have an href attribute
        for a in soup.find_all('a', href=True):
            # Resolve the href to a full absolute URL
            full_url = urljoin(next_url, a['href'])
            # Only include URLs that start with http (excludes mailto, javascript, etc.)
            if full_url.startswith('http'):
                # Add the resolved child URL to the child_urls list
                child_urls.append(full_url)

        # Look for a pagination link with rel="next" to check if there are more pages
        next_link = soup.find('a', rel='next')
        # Set next_url to the next page URL if found, otherwise set to None to end the loop
        next_url = urljoin(url, next_link['href']) if next_link else None

    # Return the collected image tags and child URLs
    return image_tags, child_urls


def save_image_tags(job_id, top_level_url, image_tags):
    # Attempt to write each image tag as its own row in the image_tags table
    try:
        # Deduplicate image tags to avoid duplicate key errors in the batch write
        unique_image_tags = list(set(image_tags))
        # Use batch_writer to efficiently write multiple items at once
        with image_tags_table.batch_writer() as batch:
            # Iterate over each unique image tag URL
            for image_url in unique_image_tags:
                # Write one row per image URL with job_id, image_url, and top_level_url
                batch.put_item(Item={
                    'job_id': job_id,
                    'image_url': image_url,
                    'top_level_url': top_level_url
                })
        # Update web_scraper_job_results to record that this top_level_url has been processed
        results_table.update_item(
            # Specify the primary key using top_level_url (partition key) and job_id (sort key)
            Key={'top_level_url': top_level_url, 'job_id': job_id},
            # Set the is_processed flag and image count for this top_level_url
            UpdateExpression='SET is_processed = :true, image_count = :count',
            # Provide the values for the update expression
            ExpressionAttributeValues={
                ':true': True,
                ':count': len(unique_image_tags)
            }
        )
    # Catch any AWS client errors that occur during the DynamoDB update
    except ClientError as e:
        # Raise a RuntimeError with a descriptive message if the save fails
        raise RuntimeError(f"Failed to save image tags: {e}")


def get_visited_urls(job_id):
    # Query the visited_urls table for all URLs associated with this job_id
    response = visited_urls_table.query(
        # Specify the partition key condition to retrieve all URLs for this job
        KeyConditionExpression=Key('job_id').eq(job_id)
    )
    # Extract the url attribute from each item and return as a set for fast lookups
    return set(item['url'] for item in response.get('Items', []))


def mark_urls_as_visited(job_id, urls):
    # Write each URL as a separate item in the visited_urls table
    with visited_urls_table.batch_writer() as batch:
        # Iterate over each URL to be marked as visited
        for url in urls:
            # Write the item with job_id (partition key) and url (sort key)
            batch.put_item(Item={'job_id': job_id, 'url': url})


def enqueue_child_urls(job_id, max_depth, current_depth, top_level_url, child_urls):
    # Fetch the current set of visited URLs for this job from DynamoDB
    visited_urls = get_visited_urls(job_id)

    # Filter out any child URLs that have already been visited to prevent circular scraping
    new_urls = [url for url in child_urls if url not in visited_urls]

    # Deduplicate new_urls to avoid duplicate keys in the batch write
    new_urls = list(set(new_urls))

    # If there are no new URLs to enqueue, return early
    if not new_urls:
        return 0

    # Mark all new URLs as visited in DynamoDB before enqueuing to minimise duplicate processing
    mark_urls_as_visited(job_id, new_urls)

    # Iterate over each new child URL and enqueue it for processing
    for child_url in new_urls:
        # Send a new SQS message for each child URL to be processed
        sqs.send_message(
            # Specify the SQS queue to send the message to
            QueueUrl=queue_url,
            # Serialise the message body as JSON containing all required scraping fields
            MessageBody=json.dumps({
                # Retain the same job_id for all child URL messages
                'job_id': job_id,
                # Retain the original max_depth so workers know when to stop
                'max_depth': max_depth,
                # Increment current_depth by 1 to track how deep we have gone
                'current_depth': current_depth + 1,
                # Retain the original top_level_url to group results correctly
                'top_level_url': top_level_url,
                # Set current_url to the child URL to be scraped next
                'current_url': child_url
            })
        )

    # Return the number of new URLs that were enqueued
    return len(new_urls)


def update_job_metadata(job_id, current_depth, max_depth, num_child_urls):
    # If not at max depth and child URLs were found, increment counts for child URLs and apply net change
    if current_depth < max_depth and num_child_urls > 0:
        # Update expression applies net change to job_remaining_urls and increments job_url_count
        update_expression = (
            'SET job_remaining_urls = job_remaining_urls + :net, '
            'job_url_count = job_url_count + :children'
        )
        # Calculate the net change to job_remaining_urls: add child URLs, subtract 1 for the processed URL
        expression_values = {
            ':net': num_child_urls - 1,
            ':children': num_child_urls
        }
    else:
        # No child URLs to enqueue — simply decrement job_remaining_urls by 1
        update_expression = 'SET job_remaining_urls = job_remaining_urls - :one'
        # Define the base expression attribute values with a decrement of 1
        expression_values = {':one': 1}

    # Apply the update to the job metadata table
    metadata_table.update_item(
        # Specify the partition key for the job record
        Key={'job_id': job_id},
        # Apply the constructed update expression
        UpdateExpression=update_expression,
        # Pass in the expression attribute values
        ExpressionAttributeValues=expression_values
    )

    # Retrieve the latest job metadata to check if the job is now complete
    result = metadata_table.get_item(Key={'job_id': job_id})
    # Check if job_remaining_urls has reached zero
    if result['Item']['job_remaining_urls'] <= 0:
        # Update the job status to COMPLETE in the metadata table
        metadata_table.update_item(
            # Specify the partition key for the job record
            Key={'job_id': job_id},
            # Set the job_status attribute to COMPLETE
            UpdateExpression='SET job_status = :status',
            # Provide the COMPLETE status as an expression attribute value
            ExpressionAttributeValues={':status': 'COMPLETE'}
        )


def process_message(message):
    # Parse the SQS message body from JSON into a Python dictionary
    body = json.loads(message['Body'])
    # Extract the receipt handle needed to delete the message after processing
    receipt_handle = message['ReceiptHandle']

    # Validate that all required fields are present in the message body
    parse_message(body)

    # Extract the job_id from the message body
    job_id = body['job_id']
    # Extract and convert max_depth to an integer
    max_depth = int(body['max_depth'])
    # Extract and convert current_depth to an integer
    current_depth = int(body['current_depth'])
    # Extract the top_level_url from the message body
    top_level_url = body['top_level_url']
    # Extract the current_url to be scraped from the message body
    current_url = body['current_url']

    # Log the current processing details for visibility
    print(f"Processing job_id={job_id} url={current_url} depth={current_depth}/{max_depth}")

    # Step 2: Mark current_url as visited to prevent it being enqueued again by another worker
    mark_urls_as_visited(job_id, [current_url])

    # Step 3: Scrape the current URL for image tags and child URLs
    image_tags, child_urls = scrape_page(current_url)

    # Step 4: Save the scraped image tags to the web_scraper_job_results table
    save_image_tags(job_id, top_level_url, image_tags)

    # Initialise child URL count to zero before conditionally enqueuing
    num_child_urls = 0
    # Step 5: Only enqueue child URLs if we have not yet reached the maximum depth
    if current_depth < max_depth:
        # Send each new (unvisited) child URL to the SQS queue for further processing
        # enqueue_child_urls returns the count of URLs actually enqueued after deduplication
        num_child_urls = enqueue_child_urls(job_id, max_depth, current_depth, top_level_url, child_urls)

    # Step 5: Update job_remaining_urls, job_url_count, and job_status in the metadata table
    update_job_metadata(job_id, current_depth, max_depth, num_child_urls)

    # Step 6: Delete the message from SQS now that it has been successfully processed
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    # Log that processing for this URL is complete
    print(f"Completed job_id={job_id} url={current_url}")


def main():
    # Log that the worker has started and is ready to poll SQS
    print("Worker started, polling SQS...")
    # Run an infinite loop to continuously poll for new messages
    while True:
        # Poll the SQS queue for the next available message
        response = sqs.receive_message(
            # Specify the queue URL to receive messages from
            QueueUrl=queue_url,
            # Request only one message at a time to avoid overloading the worker
            MaxNumberOfMessages=1,
            # Set the visibility timeout to prevent other workers from processing the same message
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            # Use long polling to wait up to 20 seconds for a message, reducing empty responses
            WaitTimeSeconds=20
        )

        # Extract the list of messages from the response, defaulting to an empty list
        messages = response.get('Messages', [])
        # If no messages were received, continue polling
        if not messages:
            continue

        # Iterate over the received messages (will be at most 1 due to MaxNumberOfMessages=1)
        for message in messages:
            # Attempt to process the message, catching any exceptions to avoid crashing the worker
            try:
                # Call the process_message function to handle the message
                process_message(message)
            # Catch any exception that occurs during message processing
            except Exception as e:
                # Log the error so it can be investigated
                print(f"Error processing message: {e}")
                # The message will automatically become visible again after the visibility timeout expires


# Entry point - only run the main function when the script is executed directly
if __name__ == '__main__':
    main()
