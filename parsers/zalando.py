from bs4 import BeautifulSoup

from models.model import ProductType, Product
from .base import HtmlParser


class ZalandoHtmlParser(HtmlParser):
    name = 'zalando'

    @staticmethod
    def _extract_prices(prices_data):
        prices = list()
        for price in prices_data:
            #  TODO price clean up using regex, get rid of "vanaf" , etc...
            cleaned_price = price.text.replace('\n', '')
            if cleaned_price != '':
                prices.append(cleaned_price)
        return prices

    def parse_product_list(self, data):
        # TODO how should we handle safe html parsing
        soup = BeautifulSoup(data['body'], features="html.parser")
        product_type_title = soup.find(id='description').find('div', {'class': 'title'}).text
        product_type_description = soup.find(id='description').find('p').text
        products_el = soup.find_all(class_='catalogArticlesList_item')
        for position, el in enumerate(products_el):
            url = el.find(class_='catalogArticlesList_productBox').attrs['href']
            brand = el.find(class_='catalogArticlesList_brandName').text
            article = el.find(class_='catalogArticlesList_articleName').text
            sku = el.find(class_='sku').text

            # Products with and without discount
            product_prices = el.find_all(class_='catalogArticlesList_price')
            prices = self._extract_prices(product_prices)
            product_type = ProductType(title=product_type_title,
                                       description=product_type_description,
                                       url=data.get('page_url'),
                                       crawled_at=data.get('crawled_at'),
                                       category=data.get('product_category'),
                                       ordering=data.get('ordering'),
                                       page_number=data.get('page_number'),
                                       position=position+1)

            product = Product(product_types=[product_type],
                              site=self.name,
                              url=url,
                              brand=brand,
                              article=article,
                              sku=sku,
                              description=None,
                              crawled_at=data['crawled_at'],
                              prices=prices)
            self._add_or_update_product(product)

    def parse_product_detail(self, data):
        soup = BeautifulSoup(data['body'], features="html.parser")
        url = data.get('page_url')
        brand = soup.find(class_='productName').find(itemprop='brand').text
        article = soup.find(class_='productName').find(itemprop='name').text
        sku = soup.find(attrs={"name": 'articleSku'})['value']

        # Products with and without discount
        product_prices = soup.find(class_='productDetailsMain').find_all(class_='price')
        prices = self._extract_prices(product_prices)
        # Parse description
        li_els = soup.find(id='productDetails').find('ul').find_all('li')
        description = '\n'.join([li.text.replace('\n', '') for li in li_els])

        product = Product(url=url,
                          site=self.name,
                          brand=brand,
                          article=article,
                          sku=sku,
                          description=description,
                          crawled_at=data['crawled_at'],
                          prices=prices)
        self._add_or_update_product(product)
