import os
import csv
from ultralytics import YOLO

# 1. Initialize
model = YOLO('yolov8n.pt') 
BASE_IMAGE_DIR = 'data/raw/images'
OUTPUT_CSV = 'data/detection_results.csv'

def classify_image(labels):
    has_person = 'person' in labels
    has_product = any(l in ['bottle', 'cup', 'box', 'vial', 'bowl'] for l in labels)
    if has_person and has_product: return 'promotional'
    elif has_product: return 'product_display'
    elif has_person: return 'lifestyle'
    return 'other'

results_list = []

print(f"Scanning base directory: {BASE_IMAGE_DIR}")

# 2. Use os.walk to find images in all subfolders
for root, dirs, files in os.walk(BASE_IMAGE_DIR):
    for img_file in files:
        if img_file.lower().endswith(('.png', '.jpg', '.jpeg')):
            img_path = os.path.join(root, img_file)
            
            # The channel name is the folder name containing the image
            channel_name = os.path.basename(root)
            msg_id = os.path.splitext(img_file)[0]
            
            print(f"Processing [{channel_name}]: {img_file}...")
            
            # Run YOLO
            results = model(img_path, conf=0.25, verbose=False)
            
            for r in results:
                labels = [model.names[int(c)] for c in r.boxes.cls]
                confs = [float(c) for c in r.boxes.conf]
                
                category = classify_image(labels)
                avg_conf = sum(confs) / len(confs) if confs else 0
                
                results_list.append([msg_id, channel_name, ", ".join(labels), avg_conf, category])

# 3. Save to CSV
with open(OUTPUT_CSV, 'w', newline='') as f:
    writer = csv.writer(f)
    # Added channel_name to the CSV for better tracking
    writer.writerow(['message_id', 'channel_name', 'detected_objects', 'confidence_score', 'image_category'])
    writer.writerows(results_list)

print(f"\nFinished! Processed {len(results_list)} images total.")
print(f"Results saved to {OUTPUT_CSV}")