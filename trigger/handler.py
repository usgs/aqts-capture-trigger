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
            parsed_body = json.loads(body)
            # handle things are coming through for the first time (i.e. they haven't failed before)
            # If they haven't been through before, they will have a `Records` key.
            if 'Record' not in parsed_body.keys():
                s3_records = json.loads(record['body'])
                for s3_record in s3_records['Records']:
                    raw_payload = {'Record': s3_record, 'resumeState': None}
                    payload = json.dumps(raw_payload)
                    process_individual_payload(payload)
            else:
                payload = body
                process_individual_payload(payload)
        else:
            raise TypeError(f'Unsupported Event Source Found: {event_source}')
    return json.loads(json.dumps({'Responses': responses}, default=serialize_datetime))
