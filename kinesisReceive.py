import json
import boto3
import base64

def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('StockDB')
    
    sns = boto3.client('sns')
    
    for record in event['Records']:
      payload=base64.b64decode(record["kinesis"]["data"]).decode('utf-8')
      data=json.loads(payload)
      
      message = {
          'id':{'S':data['id']},
          'timestamp':{'S':data['timestamp']},
          'close':{'N':str(data['close'])},
          'timestamp':{'N':str(data['timestamp'])},
          'fiftyTwoWeekHigh':{'N':str(data['fiftyTwoWeekHigh'])},
          'fiftyTwoWeekLow':{'N':str(data['fiftyTwoWeekLow'])}
      }
      
      
      if((data['close']/data['fiftyTwoWeekHigh']*100) >= 80 or (data['close']/data['fiftyTwoWeekLow']*100) <= 120):
        sns.publish(TargetArn='arn:aws:sns:us-east-1:958987441018:StockAlert',Message = json.dumps(data), MessageStructure='json')
        table.put_item(Item=message)

