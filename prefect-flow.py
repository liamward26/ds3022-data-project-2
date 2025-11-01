# prefect flow goes here
# Import necessary libraries for Prefect flow management, HTTP requests, AWS SQS operations, and time handling
from prefect import flow, task
import requests
import boto3
import time

# Task 1: Populate SQS Queue
# This task makes a POST request to the API endpoint to populate the SQS queue with 21 messages
@task(log_prints=True)
def populate_SQS_queue(url):
    """
    Populates the SQS queue with 21 messages by making a POST request to the API.
    Note: This clears existing messages and adds new ones with random DelaySeconds (30-900s).
    """
    try:
        # Make POST request to the API endpoint
        print(f"Making POST request to: {url}")
        response = requests.post(url)
        response.raise_for_status()  # Raises an exception for bad status codes
        
        # Parse and return the JSON response
        result = response.json()
        print(f"Successfully populated SQS queue. Response: {result}")
        return result
    except Exception as e:
        print(f"Error populating SQS queue: {e}")
        raise

# Task 2: Monitor Queue Messages
# This task continuously monitors the SQS queue until all messages become available (not delayed)
@task(log_prints=True)
def monitor_queue_messages(queue_url, target_count=21, check_interval=60, max_wait_time=1200):
    """
    Monitors the SQS queue and waits until we have the target number of messages.
    Checks once per minute until all messages are available.
    """
    try:
        # Initialize SQS client and tracking variables
        sqs = boto3.client('sqs')
        start_time = time.time()
        
        print(f"Starting to monitor queue for {target_count} messages...")
        print(f"Will check every {check_interval} seconds, max wait time: {max_wait_time} seconds")
        
        # Continuous monitoring loop
        while True:
            # Get queue attributes to check message count
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed']
            )
            
            # Parse message counts from queue attributes
            attributes = response['Attributes']
            available_messages = int(attributes.get('ApproximateNumberOfMessages', 0)) # make int bc it's str
            delayed_messages = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0)) # make int bc it's str
            total_messages = available_messages + delayed_messages
            
            # Calculate elapsed time and log current status
            elapsed_time = time.time() - start_time
            print(f"Time: {elapsed_time:.0f}s | Available: {available_messages} | Delayed: {delayed_messages} | Total: {total_messages}")
            
            # Check if we have all messages available (not delayed)
            if available_messages >= target_count:
                print(f"Success! All {target_count} messages are now available in the queue.")
                return {
                    'available_messages': available_messages,
                    'delayed_messages': delayed_messages,
                    'total_messages': total_messages,
                    'wait_time': elapsed_time
                }
            
            # Check if we've exceeded max wait time
            if elapsed_time > max_wait_time:
                print(f"Timeout reached. Only {available_messages}/{target_count} messages available after {max_wait_time} seconds.")
                return {
                    'available_messages': available_messages,
                    'delayed_messages': delayed_messages,
                    'total_messages': total_messages,
                    'wait_time': elapsed_time,
                    'timeout': True
                }
            
            # Wait before next check
            print(f"Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)
            
    except Exception as e:
        print(f"Error monitoring queue: {e}")
        raise

# Task 3: Collect and Parse Messages
# This task retrieves all messages from the queue, parses their attributes, and deletes them
@task(log_prints=True)
def collect_and_parse_messages(queue_url, expected_count=21):
    """
    Collects all messages from the SQS queue, parses their MessageAttributes,
    and deletes them after processing.
    """
    try:
        # Initialize SQS client and message collection list
        sqs = boto3.client('sqs')
        collected_messages = []
        
        print(f"Starting to collect {expected_count} messages from queue...")
        
        # Continue collecting until we have all expected messages
        while len(collected_messages) < expected_count:
            # Request up to 10 messages (SQS limit per request)
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(10, expected_count - len(collected_messages)),
                MessageAttributeNames=['All'],
                WaitTimeSeconds=20  # Long polling
            )
            
            # Extract messages from response
            messages = response.get('Messages', [])
            
            # Break if no more messages available
            if not messages:
                print("No more messages available at this time")
                break
            
            print(f"Received {len(messages)} messages")
            
            # Process each message individually
            for message in messages:
                try:
                    # Extract meaningful data from MessageAttributes
                    message_attrs = message.get('MessageAttributes', {})
                    
                    # check that both attributes exist and extract them
                    if 'order_no' in message_attrs and 'word' in message_attrs:
                        # Parse order number and word from message attributes
                        order_no = message_attrs['order_no']['StringValue']
                        word = message_attrs['word']['StringValue']
                        
                        # Store the paired data
                        collected_messages.append({
                            'order_no': order_no,
                            'word': word,
                            'receipt_handle': message['ReceiptHandle']
                        })
                        
                        print(f"Parsed message - Order: {order_no}, Word: {word}")
                        
                        # Delete the message from queue
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f"Deleted message with order_no: {order_no}")
                        
                    else:
                        print(f"Message missing required attributes: {message_attrs}")
                        
                except Exception as e:
                    print(f"Error processing individual message: {e}")
                    # Still delete the problematic message to avoid infinite loop (chatGPT told me to do this one :D )
                    if 'ReceiptHandle' in message:
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
            
            # Log progress and check if more messages needed
            print(f"Total messages collected so far: {len(collected_messages)}")
            
            # No additional sleep needed - long polling handles the waiting
            if len(collected_messages) < expected_count:
                remaining = expected_count - len(collected_messages)
                print(f"Still need {remaining} more messages. Continuing to poll...")
        
        # Return all collected messages
        print(f"Successfully collected {len(collected_messages)} messages")
        return collected_messages
        
    except Exception as e:
        print(f"Error collecting messages: {e}")
        raise

# Task 4: Reassemble Phrase
# This task sorts the collected messages by order number and joins the words into a complete phrase
@task(log_prints=True)
def reassemble_phrase(messages):
    """
    Takes the collected messages and reassembles them into a complete phrase
    based on their order_no values.
    """
    try:
        # Sort messages by order_no (convert to int for proper sorting)
        sorted_messages = sorted(messages, key=lambda x: int(x['order_no']))
        
        # Extract words in order
        words = [msg['word'] for msg in sorted_messages]
        
        # Join into complete phrase
        complete_phrase = ' '.join(words)
        
        print(f"Reassembled phrase: {complete_phrase}")
        
        # Return phrase information
        return {
            'complete_phrase': complete_phrase,
            'word_count': len(words),
            'sorted_messages': sorted_messages
        }
        
    except Exception as e:
        print(f"Error reassembling phrase: {e}")
        raise

# Task 5: Submit Solution
# This task submits the completed phrase to the submission queue with required metadata
@task(log_prints=True)
def submit_solution(uvaid, phrase, platform):
    """
    Submits the completed phrase to the submission queue with required attributes.
    """
    try:
        # Initialize SQS client and submission URL
        sqs = boto3.client('sqs')
        submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        
        print(f"Submitting solution - UVA ID: {uvaid}, Platform: {platform}")
        print(f"Phrase: {phrase}")
        
        # Send message to submission queue with required attributes
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody="Data Pipeline 2 Solution Submission",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        
        # Check for successful submission (HTTP 200)
        http_status = response['ResponseMetadata']['HTTPStatusCode']
        if http_status == 200:
            print(f"Solution submitted successfully :D HTTP Status: {http_status}")
            print(f"Message ID: {response['MessageId']}")
        else:
            print(f"Submission failed with HTTP Status: {http_status} :(")
        
        # Return submission details
        return {
            'message_id': response['MessageId'],
            'http_status': http_status,
            'response': response
        }
        
    except Exception as e:
        print(f"Error submitting solution: {e}")
        raise

# Main Flow: Data Pipeline
# This flow orchestrates all tasks in sequence to complete the data processing pipeline
@flow(log_prints=True)
def data_pipeline():
    # Configuration variables for API endpoints and user identification
    api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vhk7vr"
    queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/vhk7vr"
    
    uvaid = "vhk7vr" 
    platform = "prefect"
    
    # Step 1: Populate the queue
    populate_result = populate_SQS_queue(api_url)
    
    # Step 2: Monitor until all messages are available
    monitor_result = monitor_queue_messages(queue_url)
    
    # Step 3: Collect and parse all messages
    messages = collect_and_parse_messages(queue_url)
    
    # Step 4: Reassemble the complete phrase
    phrase_result = reassemble_phrase(messages)
    
    # Step 5: Submit the solution
    submission_result = submit_solution(uvaid, phrase_result['complete_phrase'], platform)
    
    # Return comprehensive pipeline results
    return {
        'populate_result': populate_result,
        'monitor_result': monitor_result,
        'messages_collected': len(messages),
        'final_phrase': phrase_result['complete_phrase'],
        'submission_result': submission_result
    }

# Entry point: Execute the pipeline when script is run directly
if __name__ == "__main__":
    result = data_pipeline()
    print(f"Pipeline completed: {result}")
