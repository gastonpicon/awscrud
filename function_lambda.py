import boto3
import json
from custom_encoder import CustomEncoder
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamoTableName = 'inventario-productos'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamoTableName)

metodoGET = 'GET'
metodoPOST = 'POST'
metodoPATCH = 'PATCH'
metodoDELETE = 'DELETE'
rutaEstado = '/health'
rutaProducto = '/product'
rutaProductos = '/products'

def lambda_handler(event, context):
    logger.info(event)
    metodoHTTP = event['httpMethod']
    ruta = event['path']
    if metodoHTTP == metodoGET and ruta == rutaEstado:
        response = buildResponse(200)
    elif metodoHTTP == metodoGET and ruta == rutaProducto:
        response = obtenerProducto(event['queryStringParameters']['productId'])    
    elif metodoHTTP == metodoGET and ruta == rutaProductos:
        response=obtenerProductos()
    elif metodoHTTP == metodoPOST and ruta == rutaProducto:
        response = guardarProducto(json.loads(event['body']))
    elif metodoHTTP == metodoPATCH and ruta == rutaProducto:
        requestBody = json.loads(event['body'])
        response=modificarProducto(requestBody['productId'], requestBody['updateKey'], requestBody['updateValue'])
    elif metodoHTTP == metodoDELETE and ruta == rutaProducto:
        requestBody = json.loads(event['body'])
        response=borrarProducto(requestBody['productId'])
    else:
        response=buildResponse(404, 'Not Found')
    
    return response

def obtenerProducto(productId):
    try:
        response = table.get_item(
            Key={
                'productId': productId
            }
        )
        if 'Item' in response:
            return buildResponse(200, response['Item'])
        else:
            return buildResponse(404, {'Message': 'ProductId: %s not found' % productId})
    except:
        logger.exception('Error desconocido!!')

def obtenerProductos():
    try:
        response = table.scan()
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])

        body = {
            'products': result
        }
        return buildResponse(200, body)
    
    except:
        logger.exception('Do your custom error handling here. I am just gonna log it out here!!')

def guardarProducto(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('Do your custom error handling here. I am just gonna log it out here!!')

def modificarProducto(productId, updateKey, updateValue):
    try:
        response=table.update_item(
            Key={
                'productId': productId
            },
            UpdateExpression='set %s = :value' % updateKey,
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
        )
        body={
            'Operation': 'UPDATE',
            'Message': 'SUCCEESS',
            'UpdatedAttributes': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Do your custom error handling here. I am just gonna log it out here!!')

def borrarProducto(productId):
    try:
        response = table.delete_item(
            Key={
                'productId': productId
            },
            ReturnValues='ALL_OLD'
        )
        body={
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'deleteItem': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Do your custom error handling here. I am just gonna log it out here!!')

def buildResponse(statusCode, body=None):
    response={
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response