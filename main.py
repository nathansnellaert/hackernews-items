import os
os.environ['CONNECTOR_NAME'] = 'hackernews-items'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')
from utils import validate_environment, upload_data
from assets.items.items import process_items

def main():
    validate_environment()
    
    # Process items - uploads are handled within the function in chunks
    process_items()

if __name__ == "__main__":
    main()