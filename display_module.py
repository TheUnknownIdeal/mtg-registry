#!/usr/bin/env python3
import matplotlib.pyplot as plt
from PIL import Image, ImageEnhance
from io import BytesIO

import requests

import scryfall_module as scryfall


def display_card_image(uuid,card_name=None, size="normal", color_flag=0):
    # The color_flag takes an integer, and can be used to code dsired changes to the image
    # Now the "color_flag" is used to change the image gray if it is above 0.

    # Request card data from Scryfall
    card_data = scryfall.uuid_fetch(uuid)
    display_image_uri(card_data, card_name=card_name, size=size, color_flag=color_flag)
    return 1


def display_image_uri(card_json, card_name=None, size="normal", color_flag=0):

    if len(card_json) != 0: # 200 response code
        if "image_uris" in card_json:
            image_url = card_json["image_uris"].get(size)

            if image_url:
                # Download the image
                img_data = requests.get(image_url).content
                # Convert image data to a PIL image
                img = Image.open(BytesIO(img_data))

                # Make image gray if color flag is above 0
                if color_flag > 0:   img = modify_image(img)

                # Set figure size to match image dimensions
                fig, ax = plt.subplots(figsize=(img.width / 100, img.height / 100), dpi=100)
                ax.imshow(img)
                ax.axis('off')  # Hide axes
                plt.subplots_adjust(left=0, right=1, top=1, bottom=0)  # Remove padding
                
                # Set window title
                fig.canvas.manager.set_window_title(card_name)

            else:
                print(f"Image size '{size}' not found for {card_name}.")

        elif "card_faces" in card_json:
            # Multi-faced card
            faces = card_json["card_faces"]
            images = []

            for face in faces:
                if "image_uris" in face:
                    image_url = face["image_uris"].get(size)
                    if image_url:
                        img_data = requests.get(image_url).content
                        img = Image.open(BytesIO(img_data))
                        if color_flag > 0:   img = modify_image(img)
                        images.append(img)

            if images:
                # Create a figure with subplots side-by-side
                fig, axes = plt.subplots(1, len(images), figsize=(sum(img.width for img in images) / 100, images[0].height / 100), dpi=100)
                
                if len(images) == 1:
                    axes = [axes]  # Ensure it's iterable if there's only one face

                for ax, img in zip(axes, images):
                    ax.imshow(img)
                    ax.axis('off')

                plt.subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)
                fig.canvas.manager.set_window_title(card_name)
        else:
            print("No image available for this card.")
    else:
        print("Card not found.")
    return 1


def desaturate_card(img, factor=0.3):
    enhancer = ImageEnhance.Color(img)
    return enhancer.enhance(factor)  # 0.0 = grayscale, 1.0 = full color

def fade_card(img):
    color = ImageEnhance.Color(img).enhance(0.4)
    brightness = ImageEnhance.Brightness(color).enhance(0.9)
    contrast = ImageEnhance.Contrast(brightness).enhance(0.9)
    return contrast


# Makes a card img object grayer
def overlay_gray(img, alpha=0.3):
    gray_overlay = Image.new('RGB', img.size, (120, 120, 120))
    return Image.blend(img, gray_overlay, alpha)

def modify_image(img):
    ret = desaturate_card(img)
    #ret = fade_card(img,alpha=0.5)
    #ret = overlay_gray(img,alpha=0.5)
    return ret