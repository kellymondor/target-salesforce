import singer
from simple_salesforce import Salesforce
from target_salesforce.transform import map_to_salesforce_fields

LOGGER = singer.get_logger()

DEFAULT_BATCH_SIZE = 1000
DEFAULT_ID_FIELD = "Id"

object_mapping = {
    "account": (lambda salesforce_client: salesforce_client.client.bulk.Account),
    "lead": (lambda salesforce_client: salesforce_client.client.bulk.Lead),
    "contact": (lambda salesforce_client: salesforce_client.client.bulk.Contact)
}

class SalesforceClient():

    def __init__(self, config, **kwargs):
        self.client = self.__get_client(config)
        self.mapping = config.get("mapping", None)
        self.id_field = config.get("id_field", DEFAULT_ID_FIELD)
        self.queues = {key: [] for key in object_mapping.keys()}
    
    def upsert(self, line, batch_size = DEFAULT_BATCH_SIZE):
        
        salesforce_object, record = map_to_salesforce_fields(self.mapping, line)
        self.queues.get(salesforce_object).append(record)
    
        if len(self.queues.get(salesforce_object)) >= batch_size:
            return self.__bulk_upsert(salesforce_object, batch_size)
    
    def flush(self):
        
        for salesforce_object, queue in self.queues.items():
            if queue:
                errors = self.__bulk_upsert(salesforce_object, len(queue))
                self.queues.get(salesforce_object).clear()
        
        return errors
    
    def __get_client(self, config):
        try:
            client = Salesforce(
                    username = config.get("username"), 
                    password = config.get("password"), 
                    security_token = config.get("security_token"),
                    domain = config.get("domain", "login")
                )
        
        except Exception as e:
            raise e
        
        return client
    
    def __bulk_upsert(self, salesforce_object, batch_size):
        
        LOGGER.info(f"Updating {batch_size} {salesforce_object} records in Salesforce.")
        
        bulk = object_mapping.get(salesforce_object)(self)
        queue = self.queues.get(salesforce_object)
        
        if bulk:
            errors = bulk.upsert(queue, self.id_field, batch_size = batch_size, use_serial = True)
            self.queues.get(salesforce_object).clear()
        else:
            raise Exception(f"Unable to load salesforce object {salesforce_object}.") 
        
        return errors