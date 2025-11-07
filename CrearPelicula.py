import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_entrada = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Procesando solicitud de creación de película",
                "event": event
            }
        }
        print(json.dumps(log_entrada))
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Salida (json)
        log_exitoso = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "tenant_id": tenant_id,
                "uuid": uuidv4,
                "pelicula": pelicula
            }
        }
        print(json.dumps(log_exitoso))
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
        
    except KeyError as e:
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error: Falta campo requerido",
                "error": str(e),
                "tipo_error": "KeyError",
                "event": event
            }
        }
        print(json.dumps(log_error))
        return {
            'statusCode': 400,
            'error': f'Campo requerido faltante: {str(e)}'
        }
        
    except Exception as e:
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error inesperado al crear película",
                "error": str(e),
                "tipo_error": type(e).__name__,
                "event": event
            }
        }
        print(json.dumps(log_error))
        return {
            'statusCode': 500,
            'error': f'Error interno: {str(e)}'
        }
