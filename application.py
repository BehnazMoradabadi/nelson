from flask import Flask, jsonify, abort
from models.model import Repository

app = Flask(__name__)

@app.route('/indices')
def indices():
    return jsonify({
        "indices": Repository.get_indices()
        })


@app.route('/brands')
def brands():
    return jsonify({
        "brands": Repository.get_brands()
        })


@app.route('/brands/<brand_name>/products')
def products_of_brand(brand_name):
    return jsonify({
        "products": Repository.get_products_of_brand(brand_name)
        })


@app.route('/products/<sku>')
def product(sku):
    product = Repository.get_product(sku)
    if not product:
        abort(404)
    return jsonify(product)


def run_api(host=None, port=None):
    app.run(host=host, port=port)