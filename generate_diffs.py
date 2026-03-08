import os
from pathlib import Path
import diff_match_patch as dmp_module

def generate_diffs_with_dmp():
    # Get environment variables
    diff_dir = os.environ.get('DIFF_DIR')
    md_dir = os.environ.get('MD_DIR')
    fin_dir = os.environ.get('FIN_DIR')
    
    # Check if environment variables are set
    if not all([diff_dir, md_dir, fin_dir]):
        print("Error: Please set DIFF_DIR, MD_DIR, and FIN_DIR environment variables")
        return
    
    # Create diff directory if it doesn't exist
    Path(diff_dir).mkdir(parents=True, exist_ok=True)
    
    # Get all files in FIN_DIR
    fin_files = [f for f in os.listdir(fin_dir) if f.endswith('_enhanced.md')]
    
    if not fin_files:
        print("No enhanced files found in FIN_DIR")
        return
    
    # Initialize diff_match_patch
    dmp = dmp_module.diff_match_patch()
    
    for fin_file in fin_files:
        # Extract base name (remove '_enhanced.md' suffix)
        base_name = fin_file.replace('_enhanced.md', '.md')
        
        # Construct full paths
        fin_path = os.path.join(fin_dir, fin_file)
        md_path = os.path.join(md_dir, base_name)
        diff_path = os.path.join(diff_dir, f"{base_name.replace('.md', '')}_diff.html")
        
        # Check if original file exists
        if not os.path.exists(md_path):
            print(f"Warning: Original file not found for {fin_file}: {md_path}")
            continue
        
        try:
            # Read both files
            with open(md_path, 'r', encoding='utf-8') as f:
                md_content = f.read()
            
            with open(fin_path, 'r', encoding='utf-8') as f:
                fin_content = f.read()
            
            # Compute diff
            diffs = dmp.diff_main(md_content, fin_content)
            dmp.diff_cleanupSemantic(diffs)
            
            # Generate HTML
            html_content = dmp.diff_prettyHtml(diffs)
            
            # Add some basic styling
            full_html = f'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Diff: {base_name}</title>
                <style>
                    body {{ font-family: monospace; margin: 20px; }}
                    .header {{ background-color: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
                    ins {{ background-color: #e0ffe0; text-decoration: none; }}
                    del {{ background-color: #ffe0e0; text-decoration: none; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>Diff: {base_name} → {fin_file}</h2>
                </div>
                {html_content}
            </body>
            </html>
            '''
            
            # Write HTML diff to file
            with open(diff_path, 'w', encoding='utf-8') as f:
                f.write(full_html)
            
            print(f"Generated HTML diff for {base_name}")
            
        except Exception as e:
            print(f"Error processing {fin_file}: {str(e)}")
    
    print(f"\nHTML diff files have been generated in: {diff_dir}")

if __name__ == "__main__":
    # First install: pip install diff-match-patch
    generate_diffs_with_dmp()