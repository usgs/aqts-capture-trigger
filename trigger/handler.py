import json
import logging
import os

import boto3

from .state_machine import AwsStepFunction

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger()
logger.setLevel(log_level)


def serialize_datetime(dt):
    return dt.isoformat()


def lambda_handler(event, context):
    logger.info(f'Event: {event}')
    logger.info(f'Context: {context}')

    state_machine_arn = os.getenv('STATE_MACHINE_ARN')
    region = os.getenv('AWS_DEPLOYMENT_REGION')
    # limit the size of the s3 objects going to the step function
    # objects greater than ~150 MB seem to cause problems
    # value is specified in bytes
    s3_object_size_limit = int(os.getenv('OBJECT_SIZE_LIMIT', 10 ** 9))

    responses = []
    transform_sfn = AwsStepFunction(state_machine_arn, region)

    def process_individual_payload(individual_payload):
        resp = transform_sfn.start_execution(invocation_payload=individual_payload)
        responses.append(resp)
        logger.info(f'State Machine Response: {resp}')

    for record in event['Records']:
        event_source = record['eventSource']
        if event_source == 'aws:sqs':
            body = record['body']
            s3_records = json.loads(body)
            try:
                s3_record_list = s3_records['Records']
            except KeyError:
                # retries from the error handler will run through this route
                payload = body
                process_individual_payload(payload)
            else:
                # handle things are coming through for the first time (i.e. they haven't failed before)
                for s3_record in s3_record_list:
                    s3_object_size = s3_record['s3']['object']['size']
                    if int(s3_object_size) < s3_object_size_limit:
                        raw_payload = {'Record': s3_record}
                        payload = json.dumps(raw_payload)
                        process_individual_payload(payload)
                    # put giant s3 files into the chopping queue
                    else:
                        send_to_chopper(s3_record, region)
        else:
            raise TypeError(f'Unsupported Event Source Found: {event_source}')
    return json.loads(json.dumps({'Responses': responses}, default=serialize_datetime))


def send_to_chopper(s3_record, region):
    sqs = boto3.client('sqs', region_name=region)
    stage = os.getenv("STAGE")
    queue_name = f"aqts-capture-chopping-queue-{stage}"
    response = sqs.get_queue_url(QueueName=queue_name)
    queue_url = response['QueueUrl']
    key = s3_record['s3']['object']['key']
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=key
    )
    logger.info(f'Putting giant file into chopping queue: {key}')
