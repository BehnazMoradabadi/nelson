import logging
import attr
from elasticsearch import Elasticsearch, helpers


logger = logging.getLogger(__name__)

@attr.s()
class ProductType:
    title = attr.ib()
    description = attr.ib()
    category = attr.ib()
    url = attr.ib()
    crawled_at = attr.ib()
    ordering = attr.ib()
    page_number = attr.ib()
    position = attr.ib()


@attr.s()
class Product:
    brand = attr.ib()
    article = attr.ib()
    sku = attr.ib()
    url = attr.ib()
    crawled_at = attr.ib()
    description = attr.ib()
    site = attr.ib()
    prices = attr.ib(default=attr.Factory(list))
    product_types = attr.ib(default=attr.Factory(list))


class Repository:
    index_name = 'product'
        
    @staticmethod
    def init_db(delete_old_db=False):
        es_client = Elasticsearch()
        keyword_type = {"type": "keyword"}
        text_type = {"type": "text"}
        not_indexed_type = {"type": "keyword", "index": False}
        body = {
            "mappings": {
                "properties": {
                    "brand": keyword_type,
                    "article": text_type,
                    "sku": keyword_type,
                    "site": keyword_type,
                    "url": not_indexed_type,
                    "crawled_at": not_indexed_type,
                    "description": text_type,
                    "prices": not_indexed_type,
                    "product_types": {
                        "type": "nested",
                        "properties": {
                            "title": text_type,
                            "description": text_type,
                            "category": text_type,
                            "url": not_indexed_type,
                            "crawled_at": not_indexed_type,
                            "ordering": not_indexed_type,
                            "page_number": not_indexed_type,
                            "position": not_indexed_type,
                        },
                    }
                }
            }
        }
        if delete_old_db:
            es_client.indices.delete('product')

        if not es_client.indices.exists('product'):
            es_client.indices.create('product', body=body)
        logger.info("db initialized succesfully")

    @staticmethod
    def bulk_save(products):
        es_client = Elasticsearch()
        update_script = '''
            if (ctx._source.product_types == null) {
                ctx._source.product_types = new ArrayList();
            }
            for (item in params.product_types) {
                ctx._source.product_types.add(item);
            }
            if (params.description != null) {
                ctx._source.description = params.description;
            }
        '''
        bulk_update_data = []
        bulk_update_script_data = []
        for product in products:
            product_dict = attr.asdict(product)
            bulk_update_script_data.append({
                  '_op_type': 'update',
                  '_index': 'product',
                  '_id': product.sku,
                  'script': {
                      'source': update_script,
                      'lang': 'painless',
                      'params': {
                          'product_types': product_dict['product_types'], 
                          'description': product_dict['description']
                        }
                   }
            })
            del product_dict['product_types']
            del product_dict['description']
            bulk_update_data.append({
                  '_op_type': 'update',
                  '_index': 'product',
                  '_id': product.sku,
                  'doc': product_dict,
                  'doc_as_upsert': True
            })


        try:
            helpers.bulk(es_client, bulk_update_data, index="product")
            helpers.bulk(es_client, bulk_update_script_data)
            logger.info('Save items %s in elastic search', len(bulk_update_data))
        except Exception as ex:
            logger.exception(ex, 'error in saving items')

    @staticmethod
    def get_indices():
        es_client = Elasticsearch()
        query = {
            "size": 0,
            "aggs": {
                "sites": {
                    "terms": {"field": "site"}
                }
            }
        }
        rs = es_client.search(index=Repository.index_name, body=query)
        indices = {}
        for item in rs.get("aggregations", {}).get("sites", {}).get("buckets", []):
            indices[item['key']] = item['doc_count']
        return indices

    @staticmethod
    def get_brands():
        es_client = Elasticsearch()
        ## TODO handle paging
        query = {
            "size": 0,
            "aggs": {
                "brands": {
                    "terms": {"field": "brand", "size": 1000}
                }
            }
        }
        rs = es_client.search(index=Repository.index_name, body=query)
        brands = {}
        for item in rs.get("aggregations", {}).get("brands", {}).get("buckets", []):
            brands[item['key']] = item['doc_count']
        return brands
    
    @staticmethod
    def get_products_of_brand(brand_name):
        es_client = Elasticsearch()
        ## TODO handle paging
        query = {
            "size": 100,
            "query": {
                "term": {"brand": brand_name}
            }
        }
        rs = es_client.search(index=Repository.index_name, body=query)
        products = []
        for item in rs.get("hits", {}).get("hits", []):
            products.append(item["_source"])
        return products
    
    @staticmethod
    def get_product(sku):
        es_client = Elasticsearch()
        ## TODO handle paging
        query = {
            "size": 100,
            "query": {
                "term": {"sku": sku}
            }
        }
        rs = es_client.search(index=Repository.index_name, body=query)
        hits = rs.get("hits", {}).get("hits", [])
        if len(hits) > 0:
            return hits[0]["_source"]
