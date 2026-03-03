import fitz  # PyMuPDF
from PIL import Image
import cv2
import numpy as np
from typing import List, Tuple, Optional
from collections import Counter
import os

def detect_table_regions(image: np.ndarray, min_table_area: int = 1000) -> List[Tuple[int, int, int, int]]:
    """
    Detect table regions in an image based on border lines.
    
    Args:
        image: OpenCV image (numpy array)
        min_table_area: Minimum area to consider as a table (removes noise)
    
    Returns:
        List of (x, y, width, height) tuples for each detected table
    """
    # Convert to grayscale if needed
    if len(image.shape) == 3:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    else:
        gray = image.copy()
    
    # Apply adaptive threshold to get binary image
    binary = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
        cv2.THRESH_BINARY_INV, 11, 2
    )
    
    # Detect horizontal lines
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (40, 1))
    horizontal_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    
    # Detect vertical lines
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 40))
    vertical_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    
    # Combine horizontal and vertical lines
    table_structure = cv2.add(horizontal_lines, vertical_lines)
    
    # Dilate to connect line segments
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    table_structure = cv2.dilate(table_structure, kernel, iterations=2)
    
    # Find contours of potential tables
    contours, _ = cv2.findContours(table_structure, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # Sort contours by y-coordinate (top to bottom), then x-coordinate (left to right)
    contours = sorted(contours, key=lambda c: (cv2.boundingRect(c)[1], cv2.boundingRect(c)[0]))
    
    table_regions = []
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        area = w * h
        
        # Filter by minimum area and aspect ratio (tables are usually rectangular)
        if area > min_table_area and w > 50 and h > 50:
            # Add padding around the table
            padding = 10
            x = max(0, x - padding)
            y = max(0, y - padding)
            w = min(image.shape[1] - x, w + 2*padding)
            h = min(image.shape[0] - y, h + 2*padding)
            
            table_regions.append((x, y, w, h))
    
    return table_regions

def extract_tables_from_pdf(pdf_path: str, dpi: int = 300, 
                           min_table_area: int = 1000,
                           save_debug_images: bool = False,
                           debug_dir: str = "debug") -> List[Image.Image]:
    """
    Extract individual tables from PDF as PIL Images.
    
    Args:
        pdf_path: Path to PDF file
        dpi: Resolution for rendering
        min_table_area: Minimum area to consider as a table
        save_debug_images: Save intermediate images for debugging
        debug_dir: Directory to save debug images
    
    Returns:
        List of PIL Images, each containing a single table
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
    
    if save_debug_images:
        os.makedirs(debug_dir, exist_ok=True)
    
    all_tables = []
    pdf_document = fitz.open(pdf_path)
    
    try:
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            
            # Render page to image
            zoom = dpi / 72
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            
            # Convert to numpy array for OpenCV
            img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                pix.height, pix.width, pix.n
            )
            
            # Convert RGB to BGR for OpenCV (if needed)
            if pix.n == 3:
                img_cv = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
            else:
                img_cv = img_array
            
            # Save full page for debugging
            if save_debug_images:
                cv2.imwrite(f"{debug_dir}/page_{page_num+1}_full.png", img_cv)
            
            # Detect table regions
            table_regions = detect_table_regions(img_cv, min_table_area)
            
            print(f"Page {page_num + 1}: Found {len(table_regions)} table(s)")
            
            # Extract each table region
            for table_idx, (x, y, w, h) in enumerate(table_regions):
                # Crop the table region
                table_cv = img_cv[y:y+h, x:x+w]
                
                # Convert back to PIL Image
                table_rgb = cv2.cvtColor(table_cv, cv2.COLOR_BGR2RGB)
                table_pil = Image.fromarray(table_rgb)
                
                all_tables.append(table_pil)
                
                # Save individual table for debugging
                if save_debug_images:
                    table_pil.save(f"{debug_dir}/page_{page_num+1}_table_{table_idx+1}.png")
            
            # Visualize detected tables on page (debug)
            if save_debug_images and table_regions:
                debug_img = img_cv.copy()
                for x, y, w, h in table_regions:
                    cv2.rectangle(debug_img, (x, y), (x+w, y+h), (0, 255, 0), 3)
                cv2.imwrite(f"{debug_dir}/page_{page_num+1}_detections.png", debug_img)
    
    finally:
        pdf_document.close()
    
    return all_tables

def batch_rotate_tables_90deg_pil(images: List[Image.Image], 
                                  output_dir: Optional[str] = None,
                                  use_majority_voting: bool = True,
                                  save_images: bool = False,
                                  debug: bool = False) -> List[Image.Image]:
    """
    Batch rotate multiple table images (PIL format), optionally using majority voting.
    
    Args:
        images: List of PIL Images
        output_dir: Directory to save images (if save_images=True)
        use_majority_voting: If True, apply same orientation to all images based on majority
        save_images: If True, save rotated images to output_dir
        debug: If True, print debug information
    
    Returns:
        List of correctly oriented PIL Images
    """
    if not images:
        return []
    
    if save_images and output_dir:
        import os
        os.makedirs(output_dir, exist_ok=True)
    
    # First pass: detect orientations for all images
    orientations = []
    image_data = []  # Store image info for processing
    
    for idx, img in enumerate(images):
        # Convert PIL to numpy for analysis
        img_np = np.array(img)
        
        # Convert to grayscale
        if len(img_np.shape) == 3:
            gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
        else:
            gray = img_np
        
        # Analyze orientation
        scores = _analyze_orientations_90deg(gray)
        best_angle = max(scores, key=scores.get)
        confidence = scores[best_angle]
        
        orientations.append(best_angle)
        image_data.append({
            'index': idx,
            'image': img,
            'angle': best_angle,
            'confidence': confidence,
            'scores': scores
        })
        
        if debug:
            print(f"Image {idx+1}: Detected {best_angle}° (confidence: {confidence:.2f})")
            print(f"  Scores: {scores}")
    
    if debug:
        print(f"\nOrientation distribution: {dict(Counter(orientations))}")
    
    # Determine final orientations
    if use_majority_voting and orientations:
        # Find most common orientation
        orientation_counts = Counter(orientations)
        common_orientation = orientation_counts.most_common(1)[0][0]
        
        if debug:
            print(f"\nMajority orientation: {common_orientation}°")
        
        # Apply majority orientation to all images
        corrected_images = []
        for data in image_data:
            if common_orientation != 0:
                # PIL rotate uses counter-clockwise positive
                corrected = data['image'].rotate(-common_orientation, expand=True, fillcolor='white')
            else:
                corrected = data['image'].copy()
            
            corrected_images.append(corrected)
            
            if save_images and output_dir:
                output_path = f"{output_dir}/rotated_{data['index']+1:03d}.png"
                corrected.save(output_path)
                
                if debug:
                    print(f"Saved: {output_path}")
    else:
        # Apply individual orientations
        corrected_images = []
        for data in image_data:
            if data['angle'] != 0:
                corrected = data['image'].rotate(-data['angle'], expand=True, fillcolor='white')
            else:
                corrected = data['image'].copy()
            
            corrected_images.append(corrected)
            
            if save_images and output_dir:
                output_path = f"{output_dir}/rotated_{data['index']+1:03d}_{data['angle']}deg.png"
                corrected.save(output_path)
                
                if debug:
                    print(f"Saved: {output_path} (rotated {data['angle']}°)")
    
    return corrected_images

def _analyze_orientations_90deg(gray: np.ndarray) -> dict:
    """
    Analyze which orientation (0°, 90°, 180°, 270°) best matches the table.
    """
    h, w = gray.shape
    
    # Apply edge detection
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    
    # Detect lines
    lines = cv2.HoughLinesP(
        edges,
        rho=1,
        theta=np.pi/180,
        threshold=50,
        minLineLength=min(h, w) // 15,  # Minimum line length
        maxLineGap=10
    )
    
    scores = {0: 0.0, 90: 0.0, 180: 0.0, 270: 0.0}
    
    if lines is not None and len(lines) > 5:
        # Classify lines by orientation
        horizontal_lines = 0
        vertical_lines = 0
        
        for line in lines:
            x1, y1, x2, y2 = line[0]
            
            # Calculate line angle
            if x2 - x1 == 0:
                angle = 90  # Vertical line
            else:
                angle = abs(np.degrees(np.arctan2(y2 - y1, x2 - x1)))
            
            # Classify as horizontal or vertical (with tolerance)
            if angle < 30 or angle > 150:  # Near horizontal
                horizontal_lines += 1
            elif 60 < angle < 120:  # Near vertical
                vertical_lines += 1
        
        total_lines = horizontal_lines + vertical_lines
        
        if total_lines > 0:
            # 0° orientation: more horizontal lines
            scores[0] = horizontal_lines / total_lines
            # 90° orientation: more vertical lines
            scores[90] = vertical_lines / total_lines
            # 180° is same as 0°
            scores[180] = scores[0]
            # 270° is same as 90°
            scores[270] = scores[90]
    
    # Also analyze texture for additional confidence
    texture_scores = _analyze_texture_orientation_90deg(gray)
    
    # Combine scores (weighted average)
    for angle in scores:
        scores[angle] = scores[angle] * 0.6 + texture_scores.get(angle, 0) * 0.4
    
    return scores

def _analyze_texture_orientation_90deg(gray: np.ndarray) -> dict:
    """
    Analyze orientation based on texture patterns.
    """
    h, w = gray.shape
    scores = {0: 0.0, 90: 0.0, 180: 0.0, 270: 0.0}
    
    # Calculate gradients
    gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    
    # Calculate gradient magnitude and angle
    mag, angle = cv2.cartToPolar(gx, gy, angleInDegrees=True)
    
    # Create orientation histogram (weighted by magnitude)
    hist, _ = np.histogram(angle, bins=36, range=(0, 360), weights=mag)
    
    # Smooth histogram
    hist = np.convolve(hist, [0.2, 0.6, 0.2], mode='same')
    
    # Calculate scores for cardinal orientations
    # 0° and 180° (horizontal edges) - bins near 0° and 180°
    scores[0] = hist[0] + hist[18]  # bins 0° and 180°
    scores[180] = scores[0]
    
    # 90° and 270° (vertical edges) - bins near 90° and 270°
    scores[90] = hist[9] + hist[27]  # bins 90° and 270°
    scores[270] = scores[90]
    
    # Normalize
    total = sum(scores.values())
    if total > 0:
        for angle in scores:
            scores[angle] /= total
    
    return scores

# Simplified version for quick use
def auto_rotate_tables_batch(images: List[Image.Image], 
                            use_majority_voting: bool = True) -> List[Image.Image]:
    """
    Simplified batch rotation for tables.
    
    Args:
        images: List of PIL Images
        use_majority_voting: If True, apply same orientation to all images
    
    Returns:
        List of rotated PIL Images
    """
    return batch_rotate_tables_90deg_pil(
        images, 
        use_majority_voting=use_majority_voting,
        save_images=False,
        debug=False
    )

# Version with confidence threshold
def batch_rotate_with_confidence(images: List[Image.Image],
                                 confidence_threshold: float = 0.6,
                                 default_orientation: int = 0) -> List[Image.Image]:
    """
    Batch rotate with confidence threshold.
    If confidence is below threshold for an image, uses default_orientation.
    
    Args:
        images: List of PIL Images
        confidence_threshold: Minimum confidence to trust detection
        default_orientation: Default orientation if confidence too low
    
    Returns:
        List of rotated PIL Images
    """
    corrected_images = []
    
    for idx, img in enumerate(images):
        # Convert to numpy for analysis
        img_np = np.array(img)
        gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY) if len(img_np.shape) == 3 else img_np
        
        # Get orientation scores
        scores = _analyze_orientations_90deg(gray)
        best_angle = max(scores, key=scores.get)
        confidence = scores[best_angle]
        
        print(f"Image {idx+1}: Best={best_angle}°, Confidence={confidence:.2f}")
        
        # Decide orientation
        if confidence >= confidence_threshold:
            final_angle = best_angle
        else:
            final_angle = default_orientation
            print(f"  Low confidence, using default {default_orientation}°")
        
        # Apply rotation
        if final_angle != 0:
            corrected = img.rotate(-final_angle, expand=True, fillcolor='white')
        else:
            corrected = img.copy()
        
        corrected_images.append(corrected)
    
    return corrected_images
