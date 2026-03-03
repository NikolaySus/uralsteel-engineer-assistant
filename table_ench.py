import os
import sys
from pathlib import Path

def replace_in_files(root_path):
    for md_file in Path(root_path).rglob("*.md"):
        content = md_file.read_text(encoding='utf-8')
        new_content = content.replace('<table>', '<table border="1">')
        
        if content != new_content:
            md_file.write_text(new_content, encoding='utf-8')
            print(f"Updated: {md_file}")

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else input("Enter path: ")
    replace_in_files(path)
