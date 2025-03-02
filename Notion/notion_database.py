from notion_client import Client
from pprint import pprint

# Use your integration secret and page ID
NOTION_TOKEN = "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"
NOTION_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

def fetch_blocks_recursive(block_id):
    """Recursively fetch all child blocks for a given block ID."""
    blocks = []
    next_cursor = None
    while True:
        response = client.blocks.children.list(block_id=block_id, start_cursor=next_cursor)
        blocks.extend(response.get("results", []))
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")
    return blocks

def print_raw_blocks(blocks):
    """Prints raw block information for debugging."""
    print(f"DEBUG: Retrieved {len(blocks)} blocks:")
    for idx, block in enumerate(blocks, start=1):
        print(f"{idx}. Type: {block.get('type')}")
        # Uncomment the next line to see full block details
        # pprint(block)

def extract_text_from_block(block):
    """Extracts text from a block based on its type; recurses into children if present."""
    block_type = block.get("type")
    # Attempt to get text for common block types:
    text_parts = block.get(block_type, {}).get("text", [])
    text = " ".join(part.get("plain_text", "") for part in text_parts).strip()
    if block.get("has_children"):
        children = fetch_blocks_recursive(block["id"])
        child_texts = [extract_text_from_block(child) for child in children]
        if child_texts:
            text += "\n" + "\n".join(child_texts).strip()
    return text.strip()

def fetch_full_page_text(page_id):
    """Fetches all blocks on the page recursively and combines their text."""
    blocks = fetch_blocks_recursive(page_id)
    print_raw_blocks(blocks)  # Debug: print raw blocks
    # Extract text from blocks that produce non-empty output.
    full_text = "\n".join(extract_text_from_block(block) for block in blocks if extract_text_from_block(block))
    return full_text

def main():
    full_text = fetch_full_page_text(NOTION_PAGE_ID)
    if not full_text:
        print("No text found on the page. Please ensure your game entries are added as text blocks (e.g., paragraphs).")
    else:
        print("Full Page Text:")
        print(full_text)

if __name__ == "__main__":
    main()