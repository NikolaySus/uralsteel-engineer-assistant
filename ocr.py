import base64
import io

def pil_image_to_base64(pil_image, format="PNG"):
    """
    Converts a PIL Image object to a Base64 encoded string.

    Args:
        pil_image (PIL.Image.Image): The PIL Image object to convert.
        format (str): The image format to use for encoding (e.g., "PNG", "JPEG").

    Returns:
        str: The Base64 encoded string of the image.
    """
    buffer = io.BytesIO()
    pil_image.save(buffer, format=format)
    img_bytes = buffer.getvalue()
    base64_string = base64.b64encode(img_bytes).decode('utf-8')
    img_base64 = "data:image/png;base64," + base64_string
    return img_base64

def qwen3vl_extract(image, client, model):
    messages = [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "You are a table OCR agent.\n"
                        "Return ONLY valid HTML for the table.\n"
                        "Rules:\n"
                        "- No markdown\n"
                        "- No explanations\n"
                        "- No surrounding text\n"
                        "- Use <table>, <tr>, <td>, <th>\n"
                        "- Preserve merged cells using colspan/rowspan\n"
                        "- Preserve empty cells\n"
                        "- Output must start with <table> and end with </table>"
                        "- /no_think"
                    )
                },
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Extract this table as HTML:"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": pil_image_to_base64(image, format="PNG")
                    }
                },
            ],
        }
    ]
    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )
    return completion.choices[0].message.content # output_text

def qwen3vl_fix(orig_img, out_img, html, client, model):
    messages = [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "You are an expert HTML table debugger. Your task is to COMPARE two images and fix the HTML code. "
                        "Focus on: Wrong cell counts? Misaligned borders? Incorrect colspan/rowspan?\n\n"
                        "PROCESS:\n"
                        "1. FIRST IMAGE: The reference/expected table layout\n"
                        "2. SECOND IMAGE: How the HTML code currently renders\n"
                        "3. HTML CODE: The table code to fix\n\n"
                        "COMPARISON CHECKLIST:\n"
                        "- Count rows and columns in both images - do they match?\n"
                        "- Check cell alignments - are cells in correct positions?\n"
                        "- Verify merged cells (colspan/rowspan) - are they correctly implemented?\n"
                        "- Look for missing or extra borders\n"
                        "- Check if text content is in correct cells\n\n"
                        "FIXING RULES:\n"
                        "- ONLY output corrected HTML code\n"
                        "- Preserve all text content\n"
                        "- Fix structural errors in table, row, or cell tags\n"
                        "- Adjust colspan/rowspan values to match reference image\n"
                        "- Add/remove cells as needed to match row/column counts\n"
                        "- Do NOT beautify or reformat unless fixing structure\n"
                        "- If images match perfectly, return the original HTML unchanged\n\n"
                        "BAD EXAMPLE (what NOT to do): Just prettifying code, adding indentation without fixing structure.\n"
                        "GOOD EXAMPLE: Changing colspan='2' to colspan='3' when image shows cell spanning 3 columns.\n\n"
                        "RESPONSE FORMAT:\n"
                        "Only output the corrected HTML code, nothing else."
                    )
                },
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Compare these two table images and fix the HTML to make the second match the first.\n\nREFERENCE (Expected):"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": pil_image_to_base64(orig_img, format="PNG")
                    }
                },
                {
                    "type": "text", 
                    "text": "CURRENT RENDER (To Fix):"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": pil_image_to_base64(out_img, format="PNG")
                    }
                },
                {
                    "type": "text", 
                    "text": f"HTML CODE TO FIX:\n```html\n{html}\n```"
                },
            ],
        }
    ]
    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )
    return completion.choices[0].message.content # output_text
