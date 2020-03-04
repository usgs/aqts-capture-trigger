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
    for record in event['Records']:
        event_source = record['eventSource']
        if event_source == 'aws:s3':
            raw_payload = {'Record': record, 'resumeState': None}
            payload = json.dumps(raw_payload)
        elif event_source == 'aws:sqs':
            payload = record['body']
        else:
            raise TypeError(f'Unsupported Event Source Found: {event_source}')
        resp = execute_state_machine(
            state_machine_arn=state_machine_arn,
            invocation_payload=payload,
            region=region
        )
        responses.append(resp)
        logger.info(f'State Machine Response: {resp}')
    return json.loads(json.dumps({'Responses': responses}, default=serialize_datetime))
