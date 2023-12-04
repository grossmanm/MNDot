import cv2
import numpy as np
from datetime import datetime
import gps

def get_intesity(img):
    return cv2.cvtColor(img, cv2.COLOR_BGR2HSV)[:,:,2]

def view_as_blocks(arr, block_shape):
    
    # Get array dimensions
    s0, s1 = arr.shape[:2]
    
    # Compute view shape
    view_shape = (s0//block_shape[0], s1//block_shape[1]) + block_shape
    
    # Create output array
    arr_out = np.zeros(view_shape)

    # Iterate through blocks and fill output array
    for i in range(0, view_shape[0]):
        for j in range(0, view_shape[1]):
            arr_out[i, j] = arr[i*block_shape[0]:(i+1)*block_shape[0], 
                                j*block_shape[1]:(j+1)*block_shape[1]]

    return arr_out


def get_saturation(img):
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    V = hsv[:,:,2]
    min_RGB = np.minimum(np.minimum(img[:,:,0], img[:,:,1]), img[:,:,2])
    S = np.where(V>0, 1 - min_RGB/V, 0)
    return S

def get_contrast(img):
    block_size = 17
    offset = 4
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    height, width = img.shape
    contrast_map = np.zeros_like(img, dtype=np.float32)

    for y in range(0, height - block_size, offset):
        for x in range(0, width - block_size, offset):
            block = img[y:y + block_size, x:x + block_size]
            mean_lum = np.mean(block)
            std_dev = np.std(block)
            rms_contrast = std_dev/ max(10, mean_lum)
            contrast_map[y:y+block_size, x:x+block_size]+=rms_contrast

    contrast_map = contrast_map/np.max(contrast_map)
    return contrast_map

def detect_circles(img):
    img = img.astype(np.uint8)
    circles = cv2.HoughCircles(img, cv2.HOUGH_GRADIENT, 1.5, 100, param1=150, param2=40, minRadius=10, maxRadius=200)
    if circles is not None:
        return circles[0]
    else:
        return np.array([])
    
def draw_gaussian(img_shape,circles):
    gaussian = np.zeros(img_shape, np.float32)

    for i in circles:
        center = (int(i[0]), int(i[1]))
        radius = i[2]

        gaussian_blob = np.zeros(img_shape)
        cv2.circle(gaussian_blob, center, int(1.5 * radius), 1, -1)
        gaussian+= gaussian_blob

    return gaussian

def detect_glare(img):
    V = get_intesity(img)
    S = get_saturation(img)
    C = get_contrast(img)
    G_photo = V * (1 - S) * (1 - C)
    circles = detect_circles(G_photo)
    #print(circles)
    G_geo = draw_gaussian(img.shape[:-1], circles)

    G = G_photo * (0.5 + 0.5*G_geo)

    return G

def main():
    img = cv2.imread('/home/malcolm/MNDot/detect_glare/2d50d202-df0a-4897-b901-72d8ef4ff25b_1920x1080.jpg')
    glare =detect_glare(img)
    num_glare = (glare/255 >=0.85).sum()
    percent_glare = num_glare/(img.shape[0]*img.shape[1])
    print(percent_glare)
if __name__ == "__main__":
    main()