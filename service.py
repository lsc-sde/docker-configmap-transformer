import kopf
import logging
import time
import asyncio
import kubernetes
import base64 
import os

apiVersion = "xlscsde.nhs.uk/v1"
kind = "ConfigMapTransform"
monitoredConfigurations = {}
configMapsMonitored = {}

kube_config = {}
kubernetes_service_host = os.environ.get("KUBERNETES_SERVICE_HOST")

if kubernetes_service_host:
    kube_config = kubernetes.config.load_incluster_config()
else:
    kube_config = kubernetes.config.load_kube_config()

api_client = kubernetes.client.ApiClient(kube_config)
core_api = kubernetes.client.CoreV1Api(api_client)
dynamic_client = kubernetes.dynamic.DynamicClient(api_client)
custom_api = dynamic_client.resources.get(api_version = apiVersion, kind = kind)

def convertToBase64(originalValue):
    originalValue_bytes = originalValue.encode("ascii") 
  
    base64_bytes = base64.b64encode(originalValue_bytes) 
    return base64_bytes.decode("ascii") 

class TransformConfigRule:
    def __init__(self, configRule): 
        self.key = configRule.get("key")
        self.type = configRule.get("type")

class TransformConfigSource:
    def __init__(self, spec, defaultNamespace): 
        sourceRef = spec.get("sourceRef")
        self.name = sourceRef.get("name")
        self.namespace = sourceRef.get("namespace") or defaultNamespace

class TransformConfig:
    def __init__(self, spec, name, namespace, body):
        self.body = body
        self.name = name
        self.namespace = namespace or "default"
        self.targetConfigMap = spec.get("targetConfigMap")
        self.source = TransformConfigSource(spec, defaultNamespace=self.namespace)
        status = body.get("status", {})
        source_status = status.get("source", {})
        self.lastResourceVersion = source_status.get("lastResourceVersion", 0)
        
        self.rules = []
        for rule in spec.get("transforms"):
            self.rules.append(TransformConfigRule(rule))

    def transformIfExists(self, override : bool = False):
        logging.info(f"Reading {self.source.name} on {self.source.namespace}")
        configMap = {}
        try:
            configMap = core_api.read_namespaced_config_map(self.source.name, self.source.namespace)
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                logging.info(f"Config Map {self.targetConfigMap} on {self.namespace} does not exist")
            else:
                raise e    
        
        if configMap:
            self.transform(configMap, override = override)

    def transform(self, configMap : kubernetes.client.V1ConfigMap, override : bool):
        resourceVersion = configMap.metadata.resource_version
        lastResourceVersion = self.lastResourceVersion

        if lastResourceVersion != resourceVersion or override:
            logging.info(f"Attempting to transform {self.source.name} on {self.source.namespace} - {resourceVersion} = {lastResourceVersion}")
            data = {}
            for rule in self.rules:
                originalValue = configMap.data.get(rule.key)
                convertedValue = convertToBase64(originalValue)
                data[rule.key] = convertedValue 
            
            
            definition = kubernetes.client.V1ConfigMap (
                metadata = kubernetes.client.V1ObjectMeta(name=self.targetConfigMap),
                data = data 
            )

            existingConfigMap = {}
            try:
                existingConfigMap=core_api.read_namespaced_config_map(name = self.targetConfigMap, namespace=self.namespace)
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == 404:
                    logging.info(f"Config Map {self.targetConfigMap} on {self.namespace} does not exist")
                else:
                    raise e    

            committedObject = {} 
            if not existingConfigMap:
                committedObject = core_api.create_namespaced_config_map(namespace=self.namespace, body = definition)
                logging.info(f"Updated {definition.metadata.name} {committedObject.metadata.resource_version}")
            else:
                committedObject = core_api.replace_namespaced_config_map(name= definition.metadata.name, namespace=self.namespace, body = definition)
                logging.info(f"Created {definition.metadata.name} {committedObject.metadata.resource_version}")

            patch = { 
                "apiVersion" : apiVersion,
                "kind" : kind,
                "metadata" : {
                    "name" : self.name,
                    "namespace" : self.namespace
                },
                "status" : { 
                    "source" : {
                        "lastResourceVersion" : resourceVersion
                    }
                }
            }


            custom_api.patch(
                body=patch, content_type="application/merge-patch+json"
            )

        else:
            logging.info(f"{self.source.name} on {self.source.namespace} does not need to be updated - {resourceVersion} = {lastResourceVersion}")
        
        configMapsMonitored[f"{self.source.namespace}/{self.source.name}"] = self
        logging.info(f"ConfigMap {self.source.name} on {self.source.namespace} is now being monitored")



@kopf.on.create("ConfigMap")
@kopf.on.update("ConfigMap")
def configMapUpdated(spec, name, namespace, **_):
    logging.info(f"Config Map {name} on {namespace} has been updated")
    config = configMapsMonitored.get(f"{namespace}/{name}")
    if config:
        logging.info(f"ConfigMap {name} on {namespace} is currently being monitored")
        config.transformIfExists()
    else:
        logging.info(f"ConfigMap {name} on {namespace} is not currently being monitored")


@kopf.on.create(kind)
@kopf.on.update(kind)
@kopf.on.resume(kind)
def configUpdated(spec, name, namespace, body, **_):
    logging.info(f"registering config {namespace}/{name}")
    config = TransformConfig(spec, name, namespace, body)
    monitoredConfigurations[f"{namespace}/{name}"] = config

    logging.info(f"registered config {namespace}/{name}")
    config.transformIfExists()