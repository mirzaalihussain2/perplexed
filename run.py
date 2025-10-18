from app import create_app

app = create_app()

if __name__ == '__main__':
    # Settings for Quart dev server
    # Prod uses Hypercorn, which has its own configuration
    app.run(
        host=app.config['QUART_HOST'],
        debug=app.config['QUART_DEBUG'],
        port=app.config['QUART_PORT']
    )