import json
import logging
import os


from .state_machine import execute_state_machine

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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
    s3_object_size_limit = int(os.getenv('OBJECT_SIZE_LIMIT', 10**9))

    responses = []

    def process_individual_payload(individual_payload):
        resp = execute_state_machine(
            state_machine_arn=state_machine_arn,
            invocation_payload=individual_payload,
            region=region
        )
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
                    raw_payload = {'Record': s3_record}
                    payload = json.dumps(raw_payload)
                    process_individual_payload(payload)
        else:
            raise TypeError(f'Unsupported Event Source Found: {event_source}')
    return json.loads(json.dumps({'Responses': responses}, default=serialize_datetime))
