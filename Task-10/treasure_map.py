import os
import cv2
from PIL import Image, ImageDraw
import numpy as np


folder = "assets"


files = sorted(os.listdir(folder))

points = []
colors = []

for file in files:
    if file.endswith(".png"):
        path = os.path.join(folder, file)
        img = cv2.imread(path)


        if np.sum(img) == 0:
            points.append(None)  
            colors.append(None)
            continue

        
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 10, 255, cv2.THRESH_BINARY)
        M = cv2.moments(thresh)

        if M["m00"] != 0:
            cx = int(M["m10"]/M["m00"])
            cy = int(M["m01"]/M["m00"])
        else:
            cx, cy = 64, 64  

        
        avg_color = cv2.mean(img)[:3]

        points.append((cx, cy))
        colors.append(avg_color)


map_size = (512, 512)
map_img = Image.new("RGB", map_size, (0, 0, 0))
draw = ImageDraw.Draw(map_img)

last_point = None
last_color = None

for i in range(len(points)):
    if points[i] is None:  
        last_point = None
        continue

    if last_point is not None:
        
        line_color = tuple(map(int, last_color))
        draw.line([last_point, points[i]], fill=line_color, width=2)

    last_point = points[i]
    last_color = colors[i]


map_img.save("treasure_path.png")
print("Treasure map saved as treasure_path.png")
