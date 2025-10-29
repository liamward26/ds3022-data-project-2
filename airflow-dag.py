from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import boto3
import time

# Default arguments for the DAG
default_args = {
    'owner': 'vhk7vr',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'data_pipeline_sqs',
    default_args=default_args,
    description='SQS message processing pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['sqs', 'data-pipeline'],
)

def populate_sqs_queue(**context):
    """
    Populates the SQS queue with 21 messages by making a POST request to the API.
    Note: This clears existing messages and adds new ones with random DelaySeconds (30-900s).
    """
    try:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vhk7vr"
        print(f"Making POST request to: {url}")
        response = requests.post(url)
        response.raise_for_status()
        
        result = response.json()
        print(f"Successfully populated SQS queue. Response: {result}")
        return result
    except Exception as e:
        print(f"Error populating SQS queue: {e}")
        raise

def monitor_queue_messages(**context):
    """
    Monitors the SQS queue and waits until we have the target number of messages.
    Checks once per minute until all messages are available.
    """
    try:
        queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/vhk7vr"
        target_count = 21
        check_interval = 60
        max_wait_time = 1200
        
        sqs = boto3.client('sqs')
        start_time = time.time()
        
        print(f"Starting to monitor queue for {target_count} messages...")
        print(f"Will check every {check_interval} seconds, max wait time: {max_wait_time} seconds")
        
        while True:
            # Get queue attributes to check message count
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesDelayed']
            )
            
            attributes = response['Attributes']
            available_messages = int(attributes.get('ApproximateNumberOfMessages', 0))
            delayed_messages = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
            total_messages = available_messages + delayed_messages
            
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

def collect_and_parse_messages(**context):
    """
    Collects all messages from the SQS queue, parses their MessageAttributes,
    and deletes them after processing.
    """
    try:
        queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/vhk7vr"
        expected_count = 21
        
        sqs = boto3.client('sqs')
        collected_messages = []
        
        print(f"Starting to collect {expected_count} messages from queue...")
        
        while len(collected_messages) < expected_count:
            # Request up to 10 messages (SQS limit per request)
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(10, expected_count - len(collected_messages)),
                MessageAttributeNames=['All'],
                WaitTimeSeconds=20  # Long polling
            )
            
            messages = response.get('Messages', [])
            
            if not messages:
                print("No more messages available at this time")
                break
            
            print(f"Received {len(messages)} messages")
            
            # Process each message
            for message in messages:
                try:
                    # Extract meaningful data from MessageAttributes
                    message_attrs = message.get('MessageAttributes', {})
                    
                    # check that both attributes exist and extract them
                    if 'order_no' in message_attrs and 'word' in message_attrs:
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
                    # Still delete the problematic message to avoid infinite loop
                    if 'ReceiptHandle' in message:
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
            
            print(f"Total messages collected so far: {len(collected_messages)}")
            
            # No additional sleep needed - long polling handles the waiting
            if len(collected_messages) < expected_count:
                remaining = expected_count - len(collected_messages)
                print(f"Still need {remaining} more messages. Continuing to poll...")
        
        print(f"Successfully collected {len(collected_messages)} messages")
        
        # Push messages to XCom for next task
        context['task_instance'].xcom_push(key='collected_messages', value=collected_messages)
        return len(collected_messages)
        
    except Exception as e:
        print(f"Error collecting messages: {e}")
        raise

def reassemble_phrase(**context):
    """
    Takes the collected messages and reassembles them into a complete phrase
    based on their order_no values.
    """
    try:
        # Pull messages from previous task
        messages = context['task_instance'].xcom_pull(key='collected_messages', task_ids='collect_messages')
        
        # Sort messages by order_no (convert to int for proper sorting)
        sorted_messages = sorted(messages, key=lambda x: int(x['order_no']))
        
        # Extract words in order
        words = [msg['word'] for msg in sorted_messages]
        
        # Join into complete phrase
        complete_phrase = ' '.join(words)
        
        print(f"Reassembled phrase: {complete_phrase}")
        
        # Push phrase to XCom for submission task
        context['task_instance'].xcom_push(key='complete_phrase', value=complete_phrase)
        
        return {
            'complete_phrase': complete_phrase,
            'word_count': len(words),
            'sorted_messages': sorted_messages
        }
        
    except Exception as e:
        print(f"Error reassembling phrase: {e}")
        raise

def submit_solution(**context):
    """
    Submits the completed phrase to the submission queue with required attributes.
    """
    try:
        # Pull phrase from previous task
        phrase = context['task_instance'].xcom_pull(key='complete_phrase', task_ids='reassemble_phrase')
        
        uvaid = "vhk7vr"
        platform = "airflow"
        
        sqs = boto3.client('sqs')
        submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        
        print(f"Submitting solution - UVA ID: {uvaid}, Platform: {platform}")
        print(f"Phrase: {phrase}")
        
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
        
        return {
            'message_id': response['MessageId'],
            'http_status': http_status,
            'response': response
        }
        
    except Exception as e:
        print(f"Error submitting solution: {e}")
        raise

# Define tasks
populate_task = PythonOperator(
    task_id='populate_queue',
    python_callable=populate_sqs_queue,
    dag=dag,
)

monitor_task = PythonOperator(
    task_id='monitor_queue',
    python_callable=monitor_queue_messages,
    dag=dag,
)

collect_task = PythonOperator(
    task_id='collect_messages',
    python_callable=collect_and_parse_messages,
    dag=dag,
)

reassemble_task = PythonOperator(
    task_id='reassemble_phrase',
    python_callable=reassemble_phrase,
    dag=dag,
)

submit_task = PythonOperator(
    task_id='submit_solution',
    python_callable=submit_solution,
    dag=dag,
)

# Define task dependencies
populate_task >> monitor_task >> collect_task >> reassemble_task >> submit_task