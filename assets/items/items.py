import asyncio
from datetime import datetime
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state, upload_data

async def fetch_item(session, item_id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    try:
        response = await session.get(url)
        response.raise_for_status()
        return await response.json()
    except Exception as e:
        print(f"Error fetching item {item_id}: {e}")
        return None

async def fetch_items_batch(start_id, batch_size, session):
    tasks = []
    for item_id in range(start_id, min(start_id + batch_size, start_id + 10000)):
        tasks.append(fetch_item(session, item_id))
    return await asyncio.gather(*tasks)

def process_items():
    import httpx
    
    # Load state to track progress
    state = load_state("items")
    last_item_id = state.get("last_item_id", 1)
    
    # Get max item ID to know when to stop
    response = get("https://hacker-news.firebaseio.com/v0/maxitem.json")
    max_item_id = response.json()
    
    print(f"Starting from item {last_item_id}, max item is {max_item_id}")
    
    # Process in chunks of 10,000
    CHUNK_SIZE = 10_000
    BATCH_SIZE = 100  # Concurrent requests
    
    current_id = last_item_id
    
    while current_id <= max_item_id:
        chunk_end = min(current_id + CHUNK_SIZE, max_item_id + 1)
        print(f"Processing items {current_id} to {chunk_end - 1}")
        
        items_data = []
        
        # Process this chunk with async batches
        async def process_chunk():
            async with httpx.AsyncClient(timeout=30.0) as session:
                batch_start = current_id
                while batch_start < chunk_end:
                    batch_items = await fetch_items_batch(batch_start, BATCH_SIZE, session)
                    for item in batch_items:
                        if item:
                            items_data.append(parse_item(item))
                    batch_start += BATCH_SIZE
                    
                    # Progress indicator
                    if batch_start % 1000 == 0:
                        print(f"  Processed up to item {batch_start}")
        
        # Run async processing
        asyncio.run(process_chunk())
        
        if items_data:
            # Create PyArrow table for this chunk
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("type", pa.string()),
                pa.field("by", pa.string()),
                pa.field("time", pa.timestamp('s')),
                pa.field("text", pa.string()),
                pa.field("dead", pa.bool_()),
                pa.field("parent", pa.int64()),
                pa.field("poll", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("score", pa.int64()),
                pa.field("title", pa.string()),
                pa.field("descendants", pa.int64()),
                pa.field("kids", pa.string()),  # JSON array as string
            ])
            
            table = pa.Table.from_pylist(items_data, schema=schema)
            
            # Upload this chunk
            print(f"Uploading {len(items_data)} items from chunk {current_id}-{chunk_end-1}")
            upload_data(table, f"items_chunk_{current_id}_{chunk_end-1}")
            
            # Update state after successful upload
            save_state("items", {"last_item_id": chunk_end - 1})
        
        current_id = chunk_end
        
        # Break if we've processed everything
        if current_id > max_item_id:
            break
    
    print(f"Completed processing up to item {min(current_id - 1, max_item_id)}")
    
    # Return empty table as all data was already uploaded in chunks
    return pa.Table.from_pylist([])

def parse_item(item):
    return {
        "id": item.get("id"),
        "type": item.get("type", ""),
        "by": item.get("by", ""),
        "time": datetime.fromtimestamp(item.get("time", 0)) if item.get("time") else None,
        "text": item.get("text", ""),
        "dead": item.get("dead", False),
        "parent": item.get("parent"),
        "poll": item.get("poll"),
        "url": item.get("url", ""),
        "score": item.get("score"),
        "title": item.get("title", ""),
        "descendants": item.get("descendants"),
        "kids": str(item.get("kids", [])) if item.get("kids") else "[]",
    }