import json
from abc import ABC, abstractmethod
import attr


@attr.s()
class HtmlParser(ABC):
    name = attr.ib(init=False)
    file_path = attr.ib()
    products = attr.ib(default=dict())

    def _get_proper_data(self, is_old, old_data=None, new_data=None):
        if new_data and (old_data is None or is_old):
            return new_data
        return old_data

    def parse(self, manager, limit):
        with open(self.file_path) as fp:
            for line in fp:
                data = json.loads(line)
                if data['page_type'] == 'product_listing':
                    self.parse_product_list(data)
                elif data['page_type'] == 'product_detail':
                    self.parse_product_detail(data)

                if len(self.products) >= limit:
                    manager.bulk_save(self.products.values())
                    self.products = {}

    def _add_or_update_product(self, updated_product):
        if updated_product.sku in self.products:
            product = self.products[updated_product.sku]
            is_old = updated_product.crawled_at > product.crawled_at
            for field in product.__dict__.keys():
                if field == 'product_types':
                    continue
                value = getattr(updated_product, field)
                if (value and
                   (is_old or getattr(product, field) is None)):
                    setattr(product, field, value)
            product.product_types.extend(updated_product.product_types)
        else:
            product = updated_product
        self.products[product.sku] = product

    @abstractmethod
    def parse_product_list(self, html_body):
        raise NotImplementedError()

    @abstractmethod
    def parse_product_detail(self, html_body):
        raise NotImplementedError()


class ParserFactory:
    from parsers.zalando import ZalandoHtmlParser
    @staticmethod
    def get_parser(file_path):
        for _class in HtmlParser.__subclasses__():
            if _class.name in file_path:
                return _class(file_path)

